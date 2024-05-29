#include "FbMessage.h"

#include <stdexcept>

namespace {
	constexpr int64_t SCALES[] = {
		0,						// 0
		10,						// 1
		100,					// 2
		1000,					// 3
		10000,					// 4
		100000,					// 5
		1000000,				// 6
		10000000,				// 7
		100000000,				// 8
		1000000000,				// 9
		10000000000,			// 10
		100000000000,			// 11
		1000000000000,			// 12
		10000000000000,			// 13
		100000000000000,		// 14
		1000000000000000,		// 15
		10000000000000000,		// 16
		100000000000000000,		// 17
		1000000000000000000		// 18
	};

	std::string to_string(int64_t value, short scale)
	{
		auto f = value / SCALES[scale];
		auto z = value % SCALES[scale];
		return "";
	}
}

namespace FirebirdHelper {

	FbMetadata::FbMetadata(
		Firebird::ThrowStatusWrapper* status,
		Firebird::IMessageMetadata* meta,
		unsigned int a_index)
		: fieldName(meta->getField(status, a_index))
		, relationName(meta->getRelation(status, a_index))
		, ownerName(meta->getOwner(status, a_index))
		, aliasName(meta->getAlias(status, a_index))
		, nullOffset(meta->getNullOffset(status, a_index))
		, offset(meta->getOffset(status, a_index))
		, type(meta->getType(status, a_index))
		, subType(meta->getSubType(status, a_index))
		, length(meta->getLength(status, a_index))
		, scale(meta->getScale(status, a_index))
		, charset(meta->getCharSet(status, a_index))
		, index(a_index)
		, nullable(meta->isNullable(status, a_index))
	{
	}

	std::string FbMetadata::sqlTypeName() const
	{
		switch (type) {
		case SQL_NULL: return "NULL";
		case SQL_BOOLEAN: return "BOOLEAN";
		case SQL_SHORT: {
			if (scale == 0)
				return "SMALLINT";
			else {
				char buf[50] = { 0 };
				snprintf(buf, sizeof(buf), "NUMERIC(4, %d)", -scale);
				return buf;
			}
		}
		case SQL_LONG: {
			if (scale == 0)
				return "INTEGER";
			else {
				char buf[50] = { 0 };
				snprintf(buf, sizeof(buf), "NUMERIC(9, %d)", -scale);
				return buf;
			}
		}
		case SQL_INT64: {
			if (scale == 0)
				return "BIGINT";
			else {
				char buf[50] = { 0 };
				snprintf(buf, sizeof(buf), "NUMERIC(18, %d)", -scale);
				return buf;
			}
		}
		case SQL_INT128: {
			if (scale == 0)
				return "INT128";
			else {
				char buf[50] = { 0 };
				snprintf(buf, sizeof(buf), "NUMERIC(38, %d)", -scale);
				return buf;
			}
		}
		case SQL_FLOAT: return "FLOAT";
		case SQL_D_FLOAT:
		case SQL_DOUBLE: {
			if (scale == 0)
				return "DOUBLE PRECISION";
			else {
				char buf[50] = { 0 };
				snprintf(buf, sizeof(buf), "NUMERIC(15, %d)", -scale);
				return buf;
			}
		}
		case SQL_DEC16: return "DECFLOAT(16)";
		case SQL_DEC34: return "DECFLOAT(34)";
		case SQL_TIMESTAMP: return "TIMESTAMP";
		case SQL_TYPE_DATE: return "DATE";
		case SQL_TYPE_TIME: return "TIME";
		case SQL_TIMESTAMP_TZ:
		case SQL_TIMESTAMP_TZ_EX:
			return "TIMESTAMP WITH TIME ZONE";
		case SQL_TIME_TZ:
		case SQL_TIME_TZ_EX:
			return "TIME WITH TIME ZONE";
		case SQL_TEXT: {
			char buf[50] = { 0 };
			if (charset == 1) {
				snprintf(buf, sizeof(buf), "BINARY(%d)", length);
				return buf;
			}
			else {
				// TODO: character set!!! Length <> octet_length
				snprintf(buf, sizeof(buf), "CHAR(%d)", length);
				return buf;
			}
		}
		case SQL_VARYING: {
			char buf[50] = { 0 };
			if (charset == 1) {
				snprintf(buf, sizeof(buf), "VARBINARY(%d)", length);
				return buf;
			}
			else {
				// TODO: character set!!! Length <> octet_length
				snprintf(buf, sizeof(buf), "VARCHAR(%d)", length);
				return buf;
			}
		}
		case SQL_BLOB:
		{
			switch (subType) {
			case 0:
				return "BLOB SUB_TYPE 0";
			case 1:
				return "BLOB SUB_TYPE TEXT";
			default: {
				char buf[50] = { 0 };
				snprintf(buf, sizeof(buf), "BLOB SUB_TYPE %d", subType);
				return buf;
			}
			}
		}
		case SQL_ARRAY:
			return "ARRAY";
		default: return "UNKNOWN";
		}
	}

	sqlda::sqlda(
		Firebird::ThrowStatusWrapper* status,
		Firebird::IMessageMetadata* meta,
		unsigned int index,
		unsigned char* messageBuffer,
		Firebird::IMaster* a_master
	)
		: FbMetadata(status, meta, index)
		, buffer(messageBuffer)
		, master(a_master)
	{
	}

	bool sqlda::getBooleanValue() const
	{
		switch (type) {
		case SQL_BOOLEAN:
			return *getValuePtr<FB_BOOLEAN>();
		case SQL_SHORT:
			return *getValuePtr<FB_BOOLEAN>() ? true : false;
		case SQL_LONG:
			return *getValuePtr<FB_BOOLEAN>() ? true : false;
		case SQL_INT64:
			return *getValuePtr<FB_BOOLEAN>() ? true : false;
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to bool";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setBooleanValue(bool value)
	{
		if (type != SQL_BOOLEAN) {
			std::string message = "Cannot convert value from bool to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		*getValuePtr<FB_BOOLEAN>() = value ? FB_TRUE : FB_FALSE;
	}

	short sqlda::getShortValue() const
	{
		if (type != SQL_SHORT) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to int16_t";
			throw std::runtime_error(message);
		}
		return *getValuePtr<ISC_SHORT>();
	}

	void sqlda::setShortValue(short value)
	{
		switch (type) {
		case SQL_SHORT:
			*getValuePtr<ISC_SHORT>() = value;
			break;
		case SQL_LONG:
			*getValuePtr<ISC_LONG>() = static_cast<ISC_LONG>(value);
			break;
		case SQL_INT64:
			*getValuePtr<ISC_INT64>() = static_cast<ISC_INT64>(value);
			break;
		default: {
			std::string message = "Cannot convert value from int16_t to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}

	int sqlda::getIntValue() const
	{
		switch (type) {
		case SQL_SHORT:
			return *getValuePtr<ISC_SHORT>();
		case SQL_LONG:
			return *getValuePtr<ISC_LONG>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to int32_t";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setIntValue(int value)
	{
		switch (type) {
		case SQL_LONG:
			*getValuePtr<ISC_LONG>() = value;
			break;
		case SQL_INT64:
			*getValuePtr<ISC_INT64>() = static_cast<ISC_INT64>(value);
			break;
		default: {
			std::string message = "Cannot convert value from int32_t to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}

	int64_t sqlda::getInt64Value() const
	{
		switch (type) {
		case SQL_SHORT:
			return *getValuePtr<ISC_SHORT>();
		case SQL_LONG:
			return *getValuePtr<ISC_LONG>();
		case SQL_INT64:
			return *getValuePtr<ISC_INT64>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to int64_t";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setInt64Value(int64_t value)
	{
		switch (type) {
		case SQL_INT64:
			*getValuePtr<ISC_INT64>() = value;
			break;
		default: {
			std::string message = "Cannot convert value from int64_t to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}

	const FB_I128* sqlda::getInt128ValuePtr() const
	{
		switch (type) {
		case SQL_INT128:
			return getValuePtr<FB_I128>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to FB_I128";
			throw std::runtime_error(message);
		}
		}
	}

	FB_I128* sqlda::getInt128ValuePtr()
	{
		switch (type) {
		case SQL_INT128:
			return getValuePtr<FB_I128>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to FB_I128";
			throw std::runtime_error(message);
		}
		}
	}

	FB_I128 sqlda::getInt128Value() const
	{
		return *getInt128ValuePtr();
	}

	void sqlda::setInt128Value(const FB_I128& value)
	{
		*getInt128ValuePtr() = value;
	}

	float sqlda::getFloatValue() const
	{
		switch (type) {
		case SQL_FLOAT:
			return *getValuePtr<float>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to float";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setFloatValue(float value)
	{
		switch (type) {
		case SQL_FLOAT:
			*getValuePtr<float>() = value;
			break;
		case SQL_D_FLOAT:
		case SQL_DOUBLE:
			*getValuePtr<double>() = value;
			break;
		default: {
			std::string message = "Cannot convert value from float to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}

	double sqlda::getDoubleValue() const
	{
		switch (type) {
		case SQL_FLOAT:
			return *getValuePtr<float>();
		case SQL_D_FLOAT:
		case SQL_DOUBLE:
			return *getValuePtr<double>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to double";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setDoubleValue(double value)
	{
		switch (type) {
		case SQL_D_FLOAT:
		case SQL_DOUBLE:
			*getValuePtr<double>() = value;
			break;
		default: {
			std::string message = "Cannot convert value from double to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}

	const FB_DEC16* sqlda::getDecFloat16ValuePtr() const {
		switch (type) {
		case SQL_DEC16:
			return getValuePtr<FB_DEC16>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to FB_DEC16";
			throw std::runtime_error(message);
		}
		}
	}

	FB_DEC16* sqlda::getDecFloat16ValuePtr() {
		switch (type) {
		case SQL_DEC16:
			return getValuePtr<FB_DEC16>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to FB_DEC16";
			throw std::runtime_error(message);
		}
		}
	}

	FB_DEC16 sqlda::getDecFloat16Value() const {
		return *getDecFloat16ValuePtr();
	}

	void sqlda::setDecFloat16Value(const FB_DEC16& value) {
		*getDecFloat16ValuePtr() = value;
	}

	const FB_DEC34* sqlda::getDecFloat34ValuePtr() const
	{
		switch (type) {
		case SQL_DEC34:
			return getValuePtr<FB_DEC34>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to FB_DEC34";
			throw std::runtime_error(message);
		}
		}
	}

	FB_DEC34* sqlda::getDecFloat34ValuePtr()
	{
		switch (type) {
		case SQL_DEC34:
			return getValuePtr<FB_DEC34>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to FB_DEC34";
			throw std::runtime_error(message);
		}
		}
	}

	FB_DEC34 sqlda::getDecFloat34Value() const
	{
		return *getDecFloat34ValuePtr();
	}

	void sqlda::setDecFloat34Value(const FB_DEC34& value)
	{
		*getDecFloat34ValuePtr() = value;
	}

	ISC_DATE sqlda::getDateValue() const
	{
		switch (type) {
		case SQL_TYPE_DATE:
			return *getValuePtr<ISC_DATE>();
		case SQL_TIMESTAMP: {
			auto ts = getValuePtr<ISC_TIMESTAMP>();
			return ts->timestamp_date;
		}
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to ISC_DATE";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setDateValue(ISC_DATE value)
	{
		switch (type) {
		case SQL_TYPE_DATE:
			*getValuePtr<ISC_DATE>() = value;
			break;
		case SQL_TIMESTAMP: {
			auto ts = getValuePtr<ISC_TIMESTAMP>();
			ts->timestamp_date = value;
		}
		default: {
			std::string message = "Cannot convert value from ISC_DATE to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}

	ISC_TIME sqlda::getTimeValue() const
	{
		switch (type) {
		case SQL_TYPE_TIME:
			return *getValuePtr<ISC_TIME>();
		case SQL_TIMESTAMP: {
			auto ts = getValuePtr<ISC_TIMESTAMP>();
			return ts->timestamp_time;
		}
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to ISC_TIME";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setTimeValue(ISC_TIME value)
	{
		switch (type) {
		case SQL_TYPE_TIME:
			*getValuePtr<ISC_TIME>() = value;
			break;
		case SQL_TIMESTAMP: {
			auto ts = getValuePtr<ISC_TIMESTAMP>();
			ts->timestamp_time = value;
		}
		default: {
			std::string message = "Cannot convert value from ISC_TIME to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}

	const ISC_TIMESTAMP* sqlda::getTimestampPtr() const
	{
		switch (type) {
		case SQL_TIMESTAMP:
			return getValuePtr<ISC_TIMESTAMP>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to ISC_TIMESTAMP";
			throw std::runtime_error(message);
		}
		}
	}

	ISC_TIMESTAMP* sqlda::getTimestampPtr()
	{
		switch (type) {
		case SQL_TIMESTAMP:
			return getValuePtr<ISC_TIMESTAMP>();
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to ISC_TIMESTAMP";
			throw std::runtime_error(message);
		}
		}
	}

	ISC_TIMESTAMP sqlda::getTimestampValue() const
	{
		return *getTimestampPtr();
	}

	void sqlda::setTimestampValue(const ISC_TIMESTAMP& value)
	{
		*getTimestampPtr() = value;
	}

	const char* sqlda::getCharPtr() const
	{
		if (type != SQL_TEXT) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to char*";
			throw std::runtime_error(message);
		}
		return getValuePtr<char>();
	}

	char* sqlda::getCharPtr()
	{
		if (type != SQL_TEXT) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to char*";
			throw std::runtime_error(message);
		}
		return getValuePtr<char>();
	}

	const unsigned char* sqlda::getBinaryPtr() const
	{
		if (type != SQL_TEXT) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to unsigned char*";
			throw std::runtime_error(message);
		}
		return getValuePtr<unsigned char>();
	}

	unsigned char* sqlda::getBinaryPtr()
	{
		if (type != SQL_TEXT) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to unsigned char*";
			throw std::runtime_error(message);
		}
		return getValuePtr<unsigned char>();
	}

	const VaryStr* sqlda::getVaryPtr() const
	{
		if (type != SQL_VARYING) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to VaryStr*";
			throw std::runtime_error(message);
		}
		return getValuePtr<VaryStr>();
	}

	VaryStr* sqlda::getVaryPtr()
	{
		if (type != SQL_VARYING) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to VaryStr*";
			throw std::runtime_error(message);
		}
		return getValuePtr<VaryStr>();
	}

	const VaryBinary* sqlda::getVarBinaryPtr() const
	{
		if (type != SQL_VARYING) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to VaryBinary*";
			throw std::runtime_error(message);
		}
		return getValuePtr<VaryBinary>();
	}

	VaryBinary* sqlda::getVarBinaryPtr()
	{
		if (type != SQL_VARYING) {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to VaryBinary*";
			throw std::runtime_error(message);
		}
		return getValuePtr<VaryBinary>();
	}

	std::string sqlda::getStringValue() const
	{
		Firebird::AutoDispose<Firebird::IStatus> st = master->getStatus();
		Firebird::ThrowStatusWrapper status(st);

		auto util = master->getUtilInterface();

		switch (type) {
		case SQL_TEXT: {
			const char* s = getCharPtr();
			return std::string(s, length);
		}
		case SQL_VARYING: {
			auto varString = getVaryPtr();
			return std::string(varString->str, varString->length);
		}
		case SQL_BOOLEAN:
			return getBooleanValue() ? "true" : "false";
		case SQL_SHORT: {
			auto shortVal = getShortValue();
			if (scale) {
				return to_string(shortVal, -scale);
			}
			else {
				return std::to_string(shortVal);
			}
		}
		case SQL_LONG: {
			auto longVal = getIntValue();
			if (scale) {
				return to_string(longVal, -scale);
			}
			else {
				return std::to_string(longVal);
			}
		}
		case SQL_INT64: {
			auto int64Val = getInt64Value();
			if (scale) {
				return to_string(int64Val, -scale);
			}
			else {
				return std::to_string(int64Val);
			}
		}
		case SQL_INT128: {
			auto int128Val = getInt128ValuePtr();
			auto i128 = util->getInt128(&status);
			std::string sInt128;
			sInt128.reserve(Firebird::IInt128::STRING_SIZE);
			i128->toString(&status, int128Val, scale, Firebird::IInt128::STRING_SIZE, sInt128.data());
			return sInt128;
		}
		case SQL_FLOAT: {
			auto floatVal = getFloatValue();
			return std::to_string(floatVal);
		}
		case SQL_DOUBLE:
		case SQL_D_FLOAT:
		{
			auto doubleVal = getDoubleValue();
			return std::to_string(doubleVal);
		}
		case SQL_DEC16: {
			auto dec16Val = getDecFloat16ValuePtr();
			auto iDec16 = util->getDecFloat16(&status);
			std::string sDec16;
			sDec16.reserve(Firebird::IDecFloat16::STRING_SIZE);
			iDec16->toString(&status, dec16Val, Firebird::IInt128::STRING_SIZE, sDec16.data());
			return sDec16;
		}
		case SQL_DEC34: {
			auto dec34Val = getDecFloat34ValuePtr();
			auto iDec34 = util->getDecFloat34(&status);
			std::string sDec34;
			sDec34.reserve(Firebird::IDecFloat34::STRING_SIZE);
			iDec34->toString(&status, dec34Val, Firebird::IInt128::STRING_SIZE, sDec34.data());
			return sDec34;
		}
		case SQL_TYPE_DATE:
		{
			auto dateValue = getDateValue();
			FbDate date{ 0, 0, 0 };
			util->decodeDate(dateValue, &date.year, &date.month, &date.day);
			char dateBuf[10];
			snprintf(dateBuf, std::size(dateBuf), "%d-%d-%d",
				date.year, date.month, date.day);
			return dateBuf;
		}
		case SQL_TYPE_TIME:
		{
			auto timeValue = getTimeValue();
			FbTime time{ 0, 0, 0, 0 };
			util->decodeTime(timeValue, &time.hours, &time.minutes, &time.seconds, &time.fractions);
			char dateBuf[15];
			snprintf(dateBuf, std::size(dateBuf), "%d:%d:%d.%d",
				time.hours, time.minutes, time.seconds, time.fractions);
			return dateBuf;
		}
		case SQL_TIMESTAMP:
		{
			auto tsValue = getTimestampValue();
			FbTimestamp ts{ 0, 0, 0, 0, 0, 0, 0 };
			util->decodeDate(tsValue.timestamp_date, &ts.year, &ts.month, &ts.day);
			util->decodeTime(tsValue.timestamp_time, &ts.hours, &ts.minutes, &ts.seconds, &ts.fractions);
			char dateBuf[26];
			snprintf(dateBuf, std::size(dateBuf), "%d-%d-%d %d:%d:%d.%d",
				ts.year, ts.month, ts.day,
				ts.hours, ts.minutes, ts.seconds, ts.fractions);
			return dateBuf;
		}
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to std::string";
			throw std::runtime_error(message);
		}
		}
	}

	void sqlda::setStringValue(std::string_view value)
	{
		switch (type) {
		case SQL_TEXT: {
			if (value.size() > static_cast<size_t>(length)) {
				throw std::runtime_error("String overflow");
			}
			char* s = getCharPtr();
			memset(s, 32, length);
			memcpy(s, value.data(), value.size());
			break;
		}
		case SQL_VARYING: {
			if (value.size() > static_cast<size_t>(length)) {
				throw std::runtime_error("String overflow");
			}
			auto varString = getVaryPtr();
			varString->length = static_cast<ISC_USHORT>(value.size());
			memcpy(varString->str, value.data(), value.size());
			break;
		}
		default: {
			std::string message = "Cannot convert value from std::string_view to ";
			message += sqlTypeName();
			throw std::runtime_error(message);
		}
		}
	}


	const ISC_QUAD* sqlda::getQuadValuePtr() const
	{
		switch (type) {
		case SQL_QUAD:
		case SQL_BLOB:
		case SQL_ARRAY:
		{
			return getValuePtr<ISC_QUAD>();
		}
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to ISC_QUAD*";
			throw std::runtime_error(message);
		}
		}
	}

	ISC_QUAD* sqlda::getQuadValuePtr()
	{
		switch (type) {
		case SQL_QUAD:
		case SQL_BLOB:
		case SQL_ARRAY:
		{
			return getValuePtr<ISC_QUAD>();
		}
		default: {
			std::string message = "Cannot convert value from ";
			message += sqlTypeName();
			message += " to ISC_QUAD*";
			throw std::runtime_error(message);
		}
		}
	}

	FbMessage::FbMessage(Firebird::ThrowStatusWrapper* status, Firebird::IMaster* master, Firebird::IMessageMetadata* meta)
		: fields()
		, buffer(meta->getMessageLength(status))
	{
		// fill sqlda
		fields.reserve(meta->getCount(status));
		auto fieldCount = meta->getCount(status);
		for (unsigned int i = 0; i < fieldCount; ++i) {
			fields.emplace_back(
				status,
				meta,
				i,
				buffer.data(),
				master
			);
		}
	}

	sqlda& FbMessage::getSqlda(size_t index)
	{
		return fields.at(index);
	}

	const sqlda& FbMessage::getSqlda(size_t index) const
	{
		return fields.at(index);
	}

	sqlda& FbMessage::operator[](size_t index)
	{
		return fields.at(index);
	}

	const sqlda& FbMessage::operator[](size_t index) const
	{
		return fields.at(index);
	}
}