#include <iostream>

#include <firebird/Interface.h>

#include "FBAutoPtr.h"
#include "FbMessage.h"

constexpr const char* db = "inet://localhost:3055/horses";

constexpr const char* sql = R"(
    SELECT 
      CODE_COLOR, -12.670
    FROM COLOR
    WHERE CODE_COLOR < ?
)";

int main()
{
	std::cout << "Hello world" << std::endl;

    Firebird::IMaster* master = Firebird::fb_get_master_interface();
	Firebird::IUtil* util = master->getUtilInterface();
	Firebird::AutoDispose<Firebird::IStatus> st = master->getStatus();
	try {
		Firebird::ThrowStatusWrapper status(st);
		Firebird::AutoRelease<Firebird::IProvider> provider = master->getDispatcher();
		Firebird::AutoDispose<Firebird::IXpbBuilder> dpbBuilder = util->getXpbBuilder(&status, Firebird::IXpbBuilder::DPB, nullptr, 0);
		//Firebird::AutoDispose<Firebird::IXpbBuilder> tpbBuilder = util->getXpbBuilder(&status, Firebird::IXpbBuilder::TPB, nullptr, 0);
		dpbBuilder->insertString(&status, isc_dpb_user_name, "SYSDBA");
		dpbBuilder->insertString(&status, isc_dpb_password, "masterkey");
		dpbBuilder->insertString(&status, isc_dpb_lc_ctype, "UTF8");
		Firebird::AutoRelease<Firebird::IAttachment> att = provider->attachDatabase(&status, db, 
			dpbBuilder->getBufferLength(&status), dpbBuilder->getBuffer(&status));

		unsigned char tpb[] = { isc_tpb_version1, isc_tpb_read, isc_tpb_read_committed, isc_tpb_read_consistency };

		Firebird::AutoRelease<Firebird::ITransaction> tra = att->startTransaction(&status, std::size(tpb), tpb);

		Firebird::AutoRelease<Firebird::IStatement> stmt = att->prepare(&status, tra, 0, sql, 3, Firebird::IStatement::PREPARE_PREFETCH_METADATA);
		
		Firebird::AutoRelease<Firebird::IMessageMetadata> inMetadata = stmt->getInputMetadata(&status);
		Firebird::AutoRelease<Firebird::IMessageMetadata> outMetadata = stmt->getOutputMetadata(&status);

		FirebirdHelper::FbMessage inMsg(&status, master, inMetadata);
		FirebirdHelper::FbMessage outMsg(&status, master, outMetadata);

		inMsg.getSqlda(0).setIntValue(20);

		Firebird::AutoRelease<Firebird::IResultSet> rs = stmt->openCursor(&status, tra, inMetadata, inMsg.data(), outMetadata, 0);

		while (rs->fetchNext(&status, outMsg.data()) == Firebird::IStatus::RESULT_OK) {
			const auto& field_1 = outMsg.getSqlda(0);
			const auto& field_2 = outMsg.getSqlda(1);
			std::cout << field_1.getAliasName() << ": " << field_1.getIntValue() << std::endl;
			std::cout << field_2.getAliasName() << ": " << field_2.getStringValue() << std::endl;
		}

		rs->close(&status);
		rs.release();


		stmt->free(&status);
		stmt.release();

		tra->commit(&status);
		tra.release();

		att->detach(&status);
		att.release();
	}
	catch (const Firebird::FbException &e) {
		std::string msg;
		msg.reserve(2048);
		util->formatStatus(msg.data(), static_cast<unsigned int>(msg.capacity()), e.getStatus());
		std::cerr << msg << std::endl;
		
	}
	return 0;
}