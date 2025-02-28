#include <iostream>
#include <chrono>

#include <firebird/Interface.h>
#include <firebird/Message.h>

#include "FBAutoPtr.h"

namespace {

	static Firebird::IMaster* master = Firebird::fb_get_master_interface();

	constexpr unsigned int MAX_SEGMENT_SIZE = 65535;

	constexpr const char* sql = R"(
select
  code_horse,
  remark
from horse
where remark is not null
)";

	std::string readBlob(Firebird::ThrowStatusWrapper* status, Firebird::IBlob* blob)
	{
		// todo: get blob size and preallocate string buffer
		std::string s;
		bool eof = false;
		std::vector<char> vBuffer(MAX_SEGMENT_SIZE);
		auto buffer = vBuffer.data();
		while (!eof) {
			unsigned int l = 0;
			switch (blob->getSegment(status, MAX_SEGMENT_SIZE, buffer, &l))
			{
			case Firebird::IStatus::RESULT_OK:
			case Firebird::IStatus::RESULT_SEGMENT:
				s.append(buffer, l);
				break;
			default:
				eof = true;
				break;
			}
		}

		return s;
	}

	void testWithoutReadBlob(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att)
	{
		using std::chrono::duration_cast;
		using std::chrono::high_resolution_clock;
		using std::chrono::milliseconds;

		std::cout << std::endl << "Test without read blob" << std::endl;

		unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

		Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(status, std::size(tpb), tpb);

		Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(status, tra, 0, sql, 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);

        if (att->VERSION >= 6u) {
            stmt->setMaxInlineBlobSize(status, 0);
            std::cout << std::format("MaxInlineBlobSize = {}", stmt->getMaxInlineBlobSize(status)) << std::endl;
        }

		Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(status);
		Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(status);

		auto t0 = high_resolution_clock::now();

		Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(status, tra, inMetadata, nullptr, outMetadata, 0);

		FB_MESSAGE(OutMessage, Firebird::ThrowStatusWrapper,
			(FB_BIGINT, code_horse)
			(FB_BLOB, remark)
		) out(status, master);

		int64_t s = 0;
		while (rs->fetchNext(status, out.getData()) == Firebird::IStatus::RESULT_OK) {
			s += out->code_horse;
		}
		auto t1 = high_resolution_clock::now();
		auto elapsed = duration_cast<milliseconds>(t1 - t0);
		std::cout << std::format("Elapsed time: {}", elapsed) << std::endl;
		std::cout << "sum: " << s << std::endl;

		rs->close(status);
		rs.release();

		stmt->free(status);
		stmt.release();

		tra->commit(status);
		tra.release();
	}

	void testWithReadBlob(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att)
	{
		using std::chrono::duration_cast;
		using std::chrono::high_resolution_clock;
		using std::chrono::milliseconds;

		std::cout << std::endl << "Test with read blob" << std::endl;

		unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

		Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(status, std::size(tpb), tpb);

		Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(status, tra, 0, sql, 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);

        if (att->VERSION >= 6u) {
            std::cout << std::format("MaxInlineBlobSize = {}", stmt->getMaxInlineBlobSize(status)) << std::endl;
        }

		Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(status);
		Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(status);

		auto t0 = high_resolution_clock::now();

		Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(status, tra, inMetadata, nullptr, outMetadata, 0);

		FB_MESSAGE(OutMessage, Firebird::ThrowStatusWrapper,
			(FB_BIGINT, code_horse)
			(FB_BLOB, remark)
		) out(status, master);

		int64_t sum = 0;
		size_t blb_size = 0;
		while (rs->fetchNext(status, out.getData()) == Firebird::IStatus::RESULT_OK) {
			sum += out->code_horse;

			Firebird::AutoRelease<Firebird::IBlob> blob = att->openBlob(status, tra, &out->remark, 0, nullptr);
			auto s = readBlob(status, blob);
			blob->close(status);
			blob.release();

			blb_size += s.size();
		}
		auto t1 = high_resolution_clock::now();
		auto elapsed = duration_cast<milliseconds>(t1 - t0);
		std::cout << std::format("Elapsed time: {}", elapsed) << std::endl;
		std::cout << "sum: " << sum << std::endl;
		std::cout << "Blob size: " << blb_size << std::endl;

		rs->close(status);
		rs.release();

		stmt->free(status);
		stmt.release();

		tra->commit(status);
		tra.release();
	}

	struct VCallback : public Firebird::IVersionCallbackImpl<VCallback, Firebird::ThrowStatusWrapper>
	{
		void callback(Firebird::ThrowStatusWrapper* status, const char* text) override
		{
			std::cout << text << std::endl;
		}
	};

	enum class OptState { NONE, DATABASE, USERNAME, PASSWORD, CHARSET, DIALECT };

	constexpr char HELP_INFO[] = R"(
Usage fbcpp-inline-blob [<database>] <options>
General options:
    -h [ --help ]                        Show help

Database options:
    -d [ --database ] connection_string  Database connection string
    -u [ --username ] user               User name
    -p [ --password ] password           Password
    -c [ --charset ] charset             Character set, default UTF8
    -s [ --sql-dialect ] dialect         SQL dialect, default 3
)";

    class TestApp final
    {
        // database options
        std::string m_database;
        std::string m_username { "SYSDBA"};
        std::string m_password { "masterkey" };
        std::string m_charset{ "UTF8" };
        unsigned short m_sqlDialect = 3;
    public:
        int exec(int argc, const char** argv);
    private:
        void printHelp()
        {
            std::cout << HELP_INFO << std::endl;
            std::exit(0);
        }

        int run();


        void parseArgs(int argc, const char** argv);
    };

    int TestApp::exec(int argc, const char** argv)
    {
        parseArgs(argc, argv);
        return run();
    }

    void TestApp::parseArgs(int argc, const char** argv)
    {
        if (argc < 2) {
            printHelp();
            exit(0);
        }
        OptState st = OptState::NONE;
        for (int i = 1; i < argc; i++) {
            std::string arg(argv[i]);
            if ((arg.size() == 2) && (arg[0] == '-')) {
                st = OptState::NONE;
                // it's option
                switch (arg[1]) {
                case 'h':
                    printHelp();
                    exit(0);
                    break;
                case 'd':
                    st = OptState::DATABASE;
                    break;
                case 'u':
                    st = OptState::USERNAME;
                    break;
                case 'p':
                    st = OptState::PASSWORD;
                    break;
                case 'c':
                    st = OptState::CHARSET;
                    break;
                case 's':
                    st = OptState::DIALECT;
                    break;
                default:
                    std::cerr << "Error: unrecognized option '" << arg << "'. See: --help" << std::endl;
                    exit(-1);
                }
            }
            else if ((arg.size() > 2) && (arg[0] == '-') && (arg[1] == '-')) {
                st = OptState::NONE;
                // it's option
                if (arg == "--help") {
                    printHelp();
                    exit(0);
                }
                if (arg == "--database") {
                    st = OptState::DATABASE;
                    continue;
                }
                if (arg == "--username") {
                    st = OptState::USERNAME;
                    continue;
                }
                if (arg == "--password") {
                    st = OptState::PASSWORD;
                    continue;
                }
                if (arg == "--charset") {
                    st = OptState::CHARSET;
                    continue;
                }
                if (arg == "--sql-dialect") {
                    st = OptState::DIALECT;
                    continue;
                }
                if (auto pos = arg.find("--database="); pos == 0) {
                    m_database.assign(arg.substr(11));
                    continue;
                }
                if (auto pos = arg.find("--username="); pos == 0) {
                    m_username.assign(arg.substr(11));
                    continue;
                }
                if (auto pos = arg.find("--password="); pos == 0) {
                    m_password.assign(arg.substr(11));
                    continue;
                }
                if (auto pos = arg.find("--charset="); pos == 0) {
                    m_charset.assign(arg.substr(10));
                    continue;
                }
                if (auto pos = arg.find("--sql-dialect="); pos == 0) {
                    std::string sql_dialect = arg.substr(14);
                    m_sqlDialect = static_cast<unsigned short>(std::stoi(sql_dialect));
                    if (m_sqlDialect != 1 && m_sqlDialect != 3) {
                        std::cerr << "Error: sql_dialect must be 1 or 3" << std::endl;
                        exit(-1);
                    }
                    continue;
                }
                std::cerr << "Error: unrecognized option '" << arg << "'. See: --help" << std::endl;
                exit(-1);
            }
            else {
                if (i == 1) {
                    m_database.assign(arg);
                    continue;
                }
                switch (st) {

                case OptState::DATABASE:
                    m_database.assign(arg);
                    break;
                case OptState::USERNAME:
                    m_username.assign(arg);
                    break;
                case OptState::PASSWORD:
                    m_password.assign(arg);
                    break;
                case OptState::CHARSET:
                    m_charset.assign(arg);
                    break;
                case OptState::DIALECT:
                    m_sqlDialect = static_cast<unsigned short>(std::stoi(arg));
                    if (m_sqlDialect != 1 && m_sqlDialect != 3) {
                        std::cerr << "Error: sql_dialect must be 1 or 3" << std::endl;
                        exit(-1);
                    }
                    break;
                default:
                    continue;
                }
            }
        }
        if (m_database.empty()) {
            std::cerr << "Error: the option '--database' is required but missing" << std::endl;
            exit(-1);
        }
    }

    int TestApp::run() {
        std::cout << "==== Blob inline test ====" << std::endl << std::endl;

        Firebird::AutoDispose<Firebird::IStatus> st = master->getStatus();
        Firebird::IUtil* util = master->getUtilInterface();
        try {
            Firebird::ThrowStatusWrapper status(st);
            Firebird::AutoRelease<Firebird::IProvider> provider = master->getDispatcher();

            Firebird::AutoDispose<Firebird::IXpbBuilder> dpbBuilder = util->getXpbBuilder(&status, Firebird::IXpbBuilder::DPB, nullptr, 0);
            //Firebird::AutoDispose<Firebird::IXpbBuilder> tpbBuilder = util->getXpbBuilder(&status, Firebird::IXpbBuilder::TPB, nullptr, 0);
            dpbBuilder->insertString(&status, isc_dpb_user_name, m_username.c_str());
            dpbBuilder->insertString(&status, isc_dpb_password, m_password.c_str());
            dpbBuilder->insertString(&status, isc_dpb_lc_ctype, m_charset.c_str());
            Firebird::AutoRelease<Firebird::IAttachment> att = provider->attachDatabase(&status, m_database.c_str(),
                dpbBuilder->getBufferLength(&status), dpbBuilder->getBuffer(&status));

            std::cout << "Firebird server version" << std::endl;
            VCallback vCallback;
            util->getFbVersion(&status, att, &vCallback);

            testWithoutReadBlob(&status, att);
            testWithReadBlob(&status, att);


            att->detach(&status);
            att.release();
        }
        catch (const Firebird::FbException& e) {
            std::string msg;
            msg.reserve(2048);
            util->formatStatus(msg.data(), static_cast<unsigned int>(msg.capacity()), e.getStatus());
            std::cout << msg << std::endl;
            return -1;
        }
        return 0;
    }

}

int main(int argc, const char** argv)
{
    TestApp app;
    return app.exec(argc, argv);
}