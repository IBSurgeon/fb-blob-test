#include <iostream>
#include <chrono>
#include <optional>

#include <firebird/Interface.h>
#include <firebird/Message.h>

#include "FBAutoPtr.h"

namespace {

	static Firebird::IMaster* master = Firebird::fb_get_master_interface();

	constexpr unsigned int MAX_SEGMENT_SIZE = 65535;
    constexpr size_t MEGABYTE = 1024 * 1024;

    constexpr const char* SQL_CACHE_WARMING = R"(
SELECT
  MAX(CHAR_LENGTH(CONTENT)) AS MAX_CHAR_LENGTH
FROM BLOB_TEST
)";

	constexpr const char* SQL_ALL_BLOB_READ = R"(
SELECT
  ID,
  CONTENT
FROM BLOB_TEST
)";

    constexpr const char* SQL_SHORT_BLOB_READ = R"(
SELECT
  ID,
  CONTENT
FROM BLOB_TEST
WHERE SHORT_BLOB IS TRUE
)";

    constexpr const char* SQL_LONG_BLOB_READ = R"(
SELECT
  ID,
  CONTENT
FROM BLOB_TEST
WHERE SHORT_BLOB IS FALSE
)";

    constexpr const char* SQL_VARCHAR_READ = R"(
SELECT
  ID,
  SHORT_CONTENT
FROM BLOB_TEST
WHERE SHORT_BLOB IS TRUE
)";

    constexpr const char* SQL_MIXED_READ = R"(
SELECT
  BLOB_TEST.ID,
  CASE
    WHEN CHAR_LENGTH(BLOB_TEST.CONTENT) <= 8191
    THEN CAST(BLOB_TEST.CONTENT AS VARCHAR(8191))
  END AS SHORT_CONTENT,
  CASE
    WHEN CHAR_LENGTH(BLOB_TEST.CONTENT) > 8191
    THEN CONTENT
  END AS CONTENT
FROM BLOB_TEST
)";

    constexpr const char* SQL_MIXED_OPT_READ = R"(
SELECT
  BLOB_TEST.ID,
  CASE
    WHEN BLOB_TEST.SHORT_BLOB IS TRUE
    THEN BLOB_TEST.SHORT_CONTENT
  END AS SHORT_CONTENT,
  CASE
    WHEN BLOB_TEST.SHORT_BLOB IS FALSE
    THEN BLOB_TEST.CONTENT
  END AS CONTENT
FROM BLOB_TEST
)";

    struct FbWireStat {
        int64_t wire_out_packets;
        int64_t wire_in_packets; 
        int64_t wire_out_bytes;
        int64_t wire_in_bytes;
        int64_t wire_snd_packets;
        int64_t wire_rcv_packets;
        int64_t wire_snd_bytes;
        int64_t wire_rcv_bytes;
        int64_t wire_roundtrips;
    };

    struct FbBlobInfo {
        int64_t blob_num_segments;
        int64_t blob_max_segment;
        int64_t blob_total_length;
        short blob_type;
    };

    enum class Read_Blob_Kind { ALL_BLOB, SHORT_BLOB, LONG_BLOB };

    const char* sql_for_blob_read_kind(Read_Blob_Kind kind)
    {
        switch (kind)
        {   
        case Read_Blob_Kind::ALL_BLOB:
            return SQL_ALL_BLOB_READ;
        case Read_Blob_Kind::SHORT_BLOB:
            return SQL_SHORT_BLOB_READ;
        case Read_Blob_Kind::LONG_BLOB:
            return SQL_LONG_BLOB_READ;
        default:
            return SQL_ALL_BLOB_READ;
        }
    }

    void getBlobStat(Firebird::ThrowStatusWrapper* status, Firebird::IBlob* blob, FbBlobInfo& stat);

	std::string readBlob(Firebird::ThrowStatusWrapper* status, Firebird::IBlob* blob)
	{
		// get blob size and preallocate string buffer
        FbBlobInfo blobInfo;
        std::memset(&blobInfo, 0, sizeof(blobInfo));
        getBlobStat(status, blob, blobInfo);

		std::string s;
        s.reserve(blobInfo.blob_total_length);
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

    int64_t portable_integer(const unsigned char* ptr, short length)
    {
        if (!ptr || length <= 0 || length > 8)
            return 0;

        int64_t value = 0;
        int shift = 0;

        while (--length > 0) {
            value += (static_cast<int64_t>(*ptr++)) << shift;
            shift += 8;
        }

        value += (static_cast<int64_t>(static_cast<char>(*ptr))) << shift;

        return value;
    }

    bool getWireStat(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, FbWireStat& stat)
    {
        ISC_UCHAR buffer[1024];
        const unsigned char info_options[] = {
            fb_info_wire_out_packets, fb_info_wire_in_packets,
            fb_info_wire_out_bytes, fb_info_wire_in_bytes,
            fb_info_wire_snd_packets, fb_info_wire_rcv_packets,
            fb_info_wire_snd_bytes, fb_info_wire_rcv_bytes,
            fb_info_wire_roundtrips, isc_info_end };

        att->getInfo(status, sizeof(info_options), info_options, sizeof(buffer), buffer);

        bool result = false;

        /* Extract the values returned in the result buffer. */
        for (ISC_UCHAR* p = buffer; *p != isc_info_end; ) {
            const unsigned char item = *p++;
            const ISC_SHORT length = static_cast<ISC_SHORT>(portable_integer(p, 2));
            p += 2;
            switch (item) {
            case fb_info_wire_out_packets:
                stat.wire_out_packets = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_in_packets:
                stat.wire_in_packets = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_out_bytes:
                stat.wire_out_bytes = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_in_bytes:
                stat.wire_in_bytes = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_snd_packets:
                stat.wire_snd_packets = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_rcv_packets:
                stat.wire_rcv_packets = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_snd_bytes:
                stat.wire_snd_bytes = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_rcv_bytes:
                stat.wire_rcv_bytes = portable_integer(p, length);
                result = true;
                break;
            case fb_info_wire_roundtrips:
                stat.wire_roundtrips = portable_integer(p, length);
                result = true;
                break;
            default:
                break;
            }
            p += length;
        };
        return result;
    }

    void getBlobStat(Firebird::ThrowStatusWrapper* status, Firebird::IBlob* blob, FbBlobInfo& stat)
    {
        ISC_UCHAR buffer[1024];
        const unsigned char info_options[] = {
            isc_info_blob_num_segments, isc_info_blob_max_segment,
            isc_info_blob_total_length, isc_info_blob_type,
            isc_info_end };
        blob->getInfo(status, sizeof(info_options), info_options, sizeof(buffer), buffer);
        /* Extract the values returned in the result buffer. */
        for (ISC_UCHAR* p = buffer; *p != isc_info_end; ) {
            const unsigned char item = *p++;
            const ISC_SHORT length = static_cast<ISC_SHORT>(portable_integer(p, 2));
            p += 2;
            switch (item) {
            case isc_info_blob_num_segments:
                stat.blob_num_segments = portable_integer(p, length);
                break;
            case isc_info_blob_max_segment:
                stat.blob_max_segment = portable_integer(p, length);
                break;
            case isc_info_blob_total_length:
                stat.blob_total_length = portable_integer(p, length);
                break;
            case isc_info_blob_type:
                stat.blob_type = static_cast<short>(portable_integer(p, length));
                break;
            default:
                break;
            }
            p += length;
        };
    }

    class WireStartCollector
    {
    private:
        FbWireStat startStat;
        FbWireStat endStat;
        bool enable = true;
    public:
        WireStartCollector() {
            memset(&startStat, 0, sizeof(startStat));
            memset(&endStat, 0, sizeof(endStat));
        }

        void startStatCollect(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att)
        {
            enable = enable && getWireStat(status, att, startStat);
        }

        void endStatCollect(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att)
        {
            enable = enable && getWireStat(status, att, endStat);
        }

        void printWireStat();
    };



    void WireStartCollector::printWireStat()
    {
        if (!enable) {
            return;
        }
        std::cout << "Wire logical statistics:" << std::endl;
        std::cout << "  send packets = " << (endStat.wire_out_packets - startStat.wire_out_packets) << std::endl;
        std::cout << "  recv packets = " << (endStat.wire_in_packets - startStat.wire_in_packets) << std::endl;
        std::cout << "  send bytes = " << (endStat.wire_out_bytes - startStat.wire_out_bytes) << std::endl;
        std::cout << "  recv bytes = " << (endStat.wire_in_bytes - startStat.wire_in_bytes) << std::endl;
        std::cout << "Wire physical statistics:" << std::endl;
        std::cout << "  send packets = " << (endStat.wire_snd_packets - startStat.wire_snd_packets) << std::endl;
        std::cout << "  recv packets = " << (endStat.wire_rcv_packets - startStat.wire_rcv_packets) << std::endl;
        std::cout << "  send bytes = " << (endStat.wire_snd_bytes - startStat.wire_snd_bytes) << std::endl;
        std::cout << "  recv bytes = " << (endStat.wire_rcv_bytes - startStat.wire_rcv_bytes) << std::endl;
        std::cout << "  roundtrips = " << (endStat.wire_roundtrips - startStat.wire_roundtrips) << std::endl;
    }

    /// <summary>
    /// Warming up the cache
    /// </summary>
    /// <param name="status">Status</param>
    /// <param name="att">Database attachment</param>
    void cacheWarmingUp(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att)
    {
        using std::chrono::duration_cast;
        using std::chrono::high_resolution_clock;
        using std::chrono::milliseconds;

        unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

        Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(status, std::size(tpb), tpb);

        std::cout << "SQL:" << std::endl << SQL_CACHE_WARMING << std::endl;

        Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(status, tra, 0, SQL_CACHE_WARMING, 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);

        Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(status);
        Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(status);

        auto t0 = high_resolution_clock::now();

        Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(status, tra, inMetadata, nullptr, outMetadata, 0);

        FB_MESSAGE(OutMessage, Firebird::ThrowStatusWrapper,
            (FB_BIGINT, MAX_CHAR_LENGTH)
        ) out(status, master);

        int64_t s = 0;
        if (rs->fetchNext(status, out.getData()) == Firebird::IStatus::RESULT_OK) {
            s = out->MAX_CHAR_LENGTH;
        }
        auto t1 = high_resolution_clock::now();
        auto elapsed = duration_cast<milliseconds>(t1 - t0);
        std::cout << std::format("Elapsed time: {}", elapsed) << std::endl;
        std::cout << "Max char length: " << s << std::endl;

        rs->close(status);
        rs.release();

        stmt->free(status);
        stmt.release();

        tra->commit(status);
        tra.release();
    }

	/// <summary>
	/// Test reading only identifiers BLOB.
	/// </summary>
	/// <param name="status">Status</param>
	/// <param name="att">Database attachment</param>
    /// <param name="readBlobKind"></param>
    /// <param name="max_inline_blob_size"></param>
    /// <param name="limit_rows"></param>
	void testReadBlobId(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, Read_Blob_Kind readBlobKind, 
        std::optional<unsigned short> max_inline_blob_size = {}, std::optional<uint64_t> limit_rows = {})
	{
		using std::chrono::duration_cast;
		using std::chrono::high_resolution_clock;
		using std::chrono::milliseconds;

		unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

		Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(status, std::size(tpb), tpb);

        std::string sql = sql_for_blob_read_kind(readBlobKind);
        if (limit_rows.has_value()) {
            sql += std::format("FETCH FIRST {} ROWS ONLY \n", limit_rows.value());
        }
        std::cout << "SQL:" << std::endl << sql << std::endl;

		Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(status, tra, 0, sql.c_str(), 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);

        if (stmt->cloopVTable->version >= stmt->VERSION) {
            if (max_inline_blob_size.has_value()) {
                stmt->setMaxInlineBlobSize(status, max_inline_blob_size.value());
            }
            std::cout << std::format("MaxInlineBlobSize = {}", stmt->getMaxInlineBlobSize(status)) << std::endl;
        }

		Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(status);
		Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(status);

        WireStartCollector wireStatCollector;

		auto t0 = high_resolution_clock::now();

        wireStatCollector.startStatCollect(status, att);

		Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(status, tra, inMetadata, nullptr, outMetadata, 0);

		FB_MESSAGE(OutMessage, Firebird::ThrowStatusWrapper,
			(FB_BIGINT, id)
			(FB_BLOB, content)
		) out(status, master);

		int64_t max_id = 0;
        int64_t record_count = 0;
		while (rs->fetchNext(status, out.getData()) == Firebird::IStatus::RESULT_OK) {
            max_id = std::max<int64_t>(max_id, out->id);
            ++record_count;
		}
        wireStatCollector.endStatCollect(status, att);

		auto t1 = high_resolution_clock::now();
		auto elapsed = duration_cast<milliseconds>(t1 - t0);
		std::cout << std::format("Elapsed time: {}", elapsed) << std::endl;
		std::cout << "Max id: " << max_id << std::endl;
        std::cout << "Record count: " << record_count << std::endl;
        wireStatCollector.printWireStat();

		rs->close(status);
		rs.release();

		stmt->free(status);
		stmt.release();

		tra->commit(status);
		tra.release();
	}

	/// <summary>
	/// Test reading BLOBs.
	/// </summary>
	/// <param name="status">Status</param>
	/// <param name="att">Database attachment</param>
	/// <param name="readBlobKind"></param>
    /// <param name="max_inline_blob_size"></param>
    /// <param name="limit_rows"></param>
    void testWithReadBlob(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, Read_Blob_Kind readBlobKind, 
        std::optional<unsigned short> max_inline_blob_size = {}, std::optional<uint64_t> limit_rows = {})
	{
		using std::chrono::duration_cast;
		using std::chrono::high_resolution_clock;
		using std::chrono::milliseconds;

		unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

		Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(status, std::size(tpb), tpb);

        std::string sql = sql_for_blob_read_kind(readBlobKind);
        if (limit_rows.has_value()) {
            sql += std::format("FETCH FIRST {} ROWS ONLY \n", limit_rows.value());
        }
        std::cout << "SQL:" << std::endl << sql << std::endl;

		Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(status, tra, 0, sql.c_str(), 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);

        if (stmt->cloopVTable->version >= stmt->VERSION) {
            if (max_inline_blob_size.has_value()) {
                stmt->setMaxInlineBlobSize(status, max_inline_blob_size.value());
            }
            std::cout << std::format("MaxInlineBlobSize = {}", stmt->getMaxInlineBlobSize(status)) << std::endl;
        }

		Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(status);
		Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(status);

        WireStartCollector wireStatCollector;

		auto t0 = high_resolution_clock::now();

        wireStatCollector.startStatCollect(status, att);

		Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(status, tra, inMetadata, nullptr, outMetadata, 0);

		FB_MESSAGE(OutMessage, Firebird::ThrowStatusWrapper,
			(FB_BIGINT, id)
			(FB_BLOB, content)
		) out(status, master);

        int64_t max_id = 0;
		size_t blb_size = 0;
        int64_t record_count = 0;
		while (rs->fetchNext(status, out.getData()) == Firebird::IStatus::RESULT_OK) {
            max_id = std::max<int64_t>(max_id, out->id);
            ++record_count;

			Firebird::AutoRelease<Firebird::IBlob> blob = att->openBlob(status, tra, &out->content, 0, nullptr);
			auto s = readBlob(status, blob);
			blob->close(status);
			blob.release();

			blb_size += s.size();
		}

        wireStatCollector.endStatCollect(status, att);

		auto t1 = high_resolution_clock::now();
		auto elapsed = duration_cast<milliseconds>(t1 - t0);
		std::cout << std::format("Elapsed time: {}", elapsed) << std::endl;
		std::cout << "Max id: " << max_id << std::endl;
        std::cout << "Record count: " << record_count << std::endl;
		std::cout << "Content size: " << blb_size << " bytes" << std::endl;
        wireStatCollector.printWireStat();

		rs->close(status);
		rs.release();

		stmt->free(status);
		stmt.release();

		tra->commit(status);
		tra.release();
	}

    /// <summary>
    /// Test reading VARCHARs.
    /// </summary>
    /// <param name="status">Status</param>
    /// <param name="att">Database attachment</param>
    void testReadVarchar(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, std::optional<uint64_t> limit_rows = {})
    {
        using std::chrono::duration_cast;
        using std::chrono::high_resolution_clock;
        using std::chrono::milliseconds;

        unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

        Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(status, std::size(tpb), tpb);

        std::string sql = SQL_VARCHAR_READ;
        if (limit_rows.has_value()) {
            sql += std::format("FETCH FIRST {} ROWS ONLY \n", limit_rows.value());
        }
        std::cout << "SQL:" << std::endl << sql << std::endl;

        Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(status, tra, 0, sql.c_str(), 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);

        Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(status);
        Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(status);

        WireStartCollector wireStatCollector;

        auto t0 = high_resolution_clock::now();

        wireStatCollector.startStatCollect(status, att);

        Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(status, tra, inMetadata, nullptr, outMetadata, 0);

        FB_MESSAGE(OutMessage, Firebird::ThrowStatusWrapper,
            (FB_BIGINT, id)
            (FB_VARCHAR(8191 * 4), short_content)
        ) out(status, master);

        int64_t max_id = 0;
        size_t blb_size = 0;
        int64_t record_count = 0;
        while (rs->fetchNext(status, out.getData()) == Firebird::IStatus::RESULT_OK) {
            max_id = std::max<int64_t>(max_id, out->id);
            ++record_count;

            blb_size += out->short_content.length;
        }
        wireStatCollector.endStatCollect(status, att);

        auto t1 = high_resolution_clock::now();
        auto elapsed = duration_cast<milliseconds>(t1 - t0);
        std::cout << std::format("Elapsed time: {}", elapsed) << std::endl;
        std::cout << "Max id: " << max_id << std::endl;
        std::cout << "Record count: " << record_count << std::endl;
        std::cout << "Content size: " << blb_size << " bytes" << std::endl;
        wireStatCollector.printWireStat();

        rs->close(status);
        rs.release();

        stmt->free(status);
        stmt.release();

        tra->commit(status);
        tra.release();
    }

    /// <summary>
    /// Test reading mixing BLOBs and VARCHARs.
    /// </summary>
    /// <param name="status">Status</param>
    /// <param name="att">Database attachment</param>
    /// <param name="optimize"></param>
    void testMixedRead(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, bool optimize, std::optional<uint64_t> limit_rows = {})
    {
        using std::chrono::duration_cast;
        using std::chrono::high_resolution_clock;
        using std::chrono::milliseconds;

        unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

        Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(status, std::size(tpb), tpb);

        std::string sql = optimize ? SQL_MIXED_OPT_READ : SQL_MIXED_READ;
        if (limit_rows.has_value()) {
            sql += std::format("FETCH FIRST {} ROWS ONLY \n", limit_rows.value());
        }
        std::cout << "SQL:" << std::endl << sql << std::endl;

        Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(status, tra, 0, sql.c_str(), 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);

        if (stmt->cloopVTable->version >= stmt->VERSION) {
            std::cout << std::format("MaxInlineBlobSize = {}", stmt->getMaxInlineBlobSize(status)) << std::endl;
        }

        Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(status);
        Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(status);

        WireStartCollector wireStatCollector;

        auto t0 = high_resolution_clock::now();

        wireStatCollector.startStatCollect(status, att);

        Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(status, tra, inMetadata, nullptr, outMetadata, 0);

        FB_MESSAGE(OutMessage, Firebird::ThrowStatusWrapper,
            (FB_BIGINT, id)
            (FB_VARCHAR(8191 * 4), short_content)
            (FB_BLOB, content)
        ) out(status, master);

        int64_t max_id = 0;
        size_t blb_size = 0;
        int64_t record_count = 0;
        while (rs->fetchNext(status, out.getData()) == Firebird::IStatus::RESULT_OK) {
            max_id = std::max<int64_t>(max_id, out->id);
            ++record_count;

            if (out->short_contentNull && !out->contentNull) {
                // Read from blob
                Firebird::AutoRelease<Firebird::IBlob> blob = att->openBlob(status, tra, &out->content, 0, nullptr);
                auto s = readBlob(status, blob);
                blob->close(status);
                blob.release();

                blb_size += s.size();
            }
            else {
                blb_size += out->short_content.length;
            }
        }
        wireStatCollector.endStatCollect(status, att);

        auto t1 = high_resolution_clock::now();
        auto elapsed = duration_cast<milliseconds>(t1 - t0);
        std::cout << std::format("Elapsed time: {}", elapsed) << std::endl;
        std::cout << "Max id: " << max_id << std::endl;
        std::cout << "Record count: " << record_count << std::endl;
        std::cout << "Content size: " << blb_size << " bytes" << std::endl;
        wireStatCollector.printWireStat();

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

	enum class OptState { NONE, DATABASE, USERNAME, PASSWORD, CHARSET, MAX_INLINE_BLOB_SIZE, ROWS_LIMIT };

	constexpr char HELP_INFO[] = R"(
Usage fb-blob-test [<database>] <options>
General options:
    -h [ --help ]                        Show help

Database options:
    -d [ --database ] connection_string  Database connection string
    -u [ --username ] user               User name
    -p [ --password ] password           Password
    -c [ --charset ] charset             Character set, default UTF8
    -z [ --compress ]                    Wire compression, default False
    -n [ --limit-rows ] value            Limit of rows
    -i [ --max-inline-blob-size ] value  Maximum inline blob size, default 65535
)";

    class TestApp final
    {
        // database options
        std::string m_database;
        std::string m_username { "SYSDBA"};
        std::string m_password { "masterkey" };
        std::string m_charset{ "UTF8" };
        std::optional<unsigned short> m_max_inline_blob_size;
        std::optional<uint64_t> m_limit_rows;
        bool m_wireCompression = false;
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
                case 'i':
                    st = OptState::MAX_INLINE_BLOB_SIZE;
                    break;
                case 'n':
                    st = OptState::ROWS_LIMIT;
                    break;
                case 'z':
                    m_wireCompression = true;
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
                if (arg == "--max-inline-blob-size") {
                    st = OptState::MAX_INLINE_BLOB_SIZE;
                    continue;
                }
                if (arg == "--limit-rows") {
                    st = OptState::ROWS_LIMIT;
                    continue;
                }
                if (arg == "--compress") {
                    m_wireCompression = true;
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
                if (auto pos = arg.find("--max-inline-blob-size="); pos == 0) {
                    std::string s_inline_size = arg.substr(23);
                    m_max_inline_blob_size = static_cast<unsigned short>(std::stoi(s_inline_size));
                    continue;
                }
                if (auto pos = arg.find("--limit-rows="); pos == 0) {
                    std::string s_limit_rows = arg.substr(14);
                    m_limit_rows = static_cast<uint64_t>(std::stoull(s_limit_rows));
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
                case OptState::MAX_INLINE_BLOB_SIZE:
                    m_max_inline_blob_size = static_cast<unsigned short>(std::stoi(arg));
                    break;
                case OptState::ROWS_LIMIT:
                    m_limit_rows = static_cast<uint64_t>(std::stoull(arg));
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

    int TestApp::run() 
    {
        std::cout << "===== Test of BLOBs transmission over the network =====" << std::endl << std::endl;

        Firebird::AutoDispose<Firebird::IStatus> st = master->getStatus();
        Firebird::IUtil* util = master->getUtilInterface();
        try {
            Firebird::ThrowStatusWrapper status(st);
            Firebird::AutoRelease<Firebird::IProvider> provider = master->getDispatcher();

            Firebird::AutoDispose<Firebird::IXpbBuilder> dpbBuilder = util->getXpbBuilder(&status, Firebird::IXpbBuilder::DPB, nullptr, 0);
            dpbBuilder->insertString(&status, isc_dpb_user_name, m_username.c_str());
            dpbBuilder->insertString(&status, isc_dpb_password, m_password.c_str());
            dpbBuilder->insertString(&status, isc_dpb_lc_ctype, m_charset.c_str());
            if (m_wireCompression) {
                dpbBuilder->insertString(&status, isc_dpb_config, "WireCompression=True");
            }
            //dpbBuilder->insertBigInt(&status, isc_dpb_max_blob_cache_size, 10 * MEGABYTE);
            //dpbBuilder->insertBigInt(&status, isc_dpb_max_inline_blob_size, 65535);

            Firebird::AutoRelease<Firebird::IAttachment> att = provider->attachDatabase(&status, m_database.c_str(),
                dpbBuilder->getBufferLength(&status), dpbBuilder->getBuffer(&status));

            std::cout << "Firebird server version" << std::endl;
            VCallback vCallback;
            util->getFbVersion(&status, att, &vCallback);

            std::cout << std::endl << "** Warming up the cache **" << std::endl;
            std::cout << "------------------------------------------------------------------------------------" << std::endl;
            cacheWarmingUp(&status, att);

            std::cout << std::endl << "** Test read short BLOBs **" << std::endl;
            std::cout << "------------------------------------------------------------------------------------" << std::endl;
            testWithReadBlob(&status, att, Read_Blob_Kind::SHORT_BLOB, m_max_inline_blob_size, m_limit_rows);

            std::cout << std::endl << "** Test read VARCHAR(8191) **" << std::endl;
            std::cout << "------------------------------------------------------------------------------------" << std::endl;
            testReadVarchar(&status, att);

            std::cout << std::endl << "** Test read all BLOBs **" << std::endl;
            std::cout << "------------------------------------------------------------------------------------" << std::endl;
            testWithReadBlob(&status, att, Read_Blob_Kind::ALL_BLOB, m_max_inline_blob_size, m_limit_rows);

            std::cout << std::endl << "** Test read mixed BLOBs and VARCHARs **" << std::endl;
            std::cout << "------------------------------------------------------------------------------------" << std::endl;
            testMixedRead(&status, att, false, m_limit_rows);

            std::cout << std::endl << "** Test read mixed BLOBs and VARCHARs with optimize **" << std::endl;
            std::cout << "------------------------------------------------------------------------------------" << std::endl;
            testMixedRead(&status, att, true, m_limit_rows);


            std::cout << std::endl << "** Test read only BLOB IDs **" << std::endl;
            std::cout << "------------------------------------------------------------------------------------" << std::endl;
            testReadBlobId(&status, att, Read_Blob_Kind::ALL_BLOB, m_max_inline_blob_size, m_limit_rows);


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