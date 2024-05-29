#pragma once

#include <string>
#include <vector>

#include <firebird/Interface.h>

#include "FBAutoPtr.h"

namespace FirebirdHelper {

    struct VaryStr {
        ISC_USHORT length;
        char str[1];
    };

    struct VaryBinary {
        ISC_USHORT length;
        unsigned char str[1];
    };

    struct FbDate {
        unsigned int year;
        unsigned int month;
        unsigned int day;
    };

    struct FbTime {
        unsigned int hours;
        unsigned int minutes;
        unsigned int seconds;
        unsigned int fractions;
    };

    struct FbTimestamp {
        unsigned int year;
        unsigned int month;
        unsigned int day;
        unsigned int hours;
        unsigned int minutes;
        unsigned int seconds;
        unsigned int fractions;
    };

    class FbMetadata {
    public:
        FbMetadata(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IMessageMetadata* meta,
            unsigned int a_index);

        const std::string& getFieldName() const &
        {
            return fieldName;
        }

        const std::string& getRelationName() const &
        {
            return relationName;
        }

        const std::string& getOwnerName() const &
        {
            return ownerName;
        }

        const std::string& getAliasName() const &
        {
            return aliasName;
        }

        unsigned int getNullOffset() const noexcept
        {
            return nullOffset;
        }

        unsigned int getOffset() const noexcept
        {
            return offset;
        }

        unsigned int getType() const noexcept
        {
            return type;
        }

        unsigned int getSubType() const noexcept
        {
            return subType;
        }

        unsigned int getLength() const noexcept
        {
            return length;
        }

        int getScale() const noexcept
        {
            return scale;
        }

        unsigned int getCharSet() const noexcept
        {
            return charset;
        }

        unsigned int getIndex() const noexcept
        {
            return index;
        }

        bool isNullable() const noexcept
        {
            return nullable;
        }

        std::string sqlTypeName() const;

    protected:
        std::string fieldName;
        std::string relationName;
        std::string ownerName;
        std::string aliasName;
        unsigned int nullOffset;
        unsigned int offset;
        unsigned int type;
        unsigned int subType;
        unsigned int length;
        int scale;
        unsigned int charset;
        unsigned int index;
        bool nullable;
    };

    class sqlda : public FbMetadata
    {
    public:
        sqlda(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IMessageMetadata* meta,
            unsigned int index,
            unsigned char* messageBuffer,
            Firebird::IMaster* a_master
        );

        bool isNull() const
        {
            short nullFlag = *reinterpret_cast<const short*>(buffer + nullOffset);
            return (nullFlag == -1) ? true : false;
        }

        void setNull(bool nullFlag) const
        {
            *reinterpret_cast<short*>(buffer + nullOffset) = nullFlag ? -1 : 0;
        }

        template <typename T>
        T* getValuePtr() const
        {
            return reinterpret_cast<T*>(buffer + offset);
        }

        // BOOLEAN
        bool getBooleanValue() const;
        void setBooleanValue(bool value);
        // SMALLINT
        short getShortValue() const;
        void setShortValue(short value);
        // INTEGER
        int getIntValue() const;
        void setIntValue(int value);
        // BIGINT
        int64_t getInt64Value() const;
        void setInt64Value(int64_t value);
        // INT128
        const FB_I128* getInt128ValuePtr() const;
        FB_I128* getInt128ValuePtr();
        FB_I128 getInt128Value() const;
        void setInt128Value(const FB_I128& value);
        // FLOAT
        float getFloatValue() const;
        void setFloatValue(float value);
        // DOUBLE PRECISION
        double getDoubleValue() const;
        void setDoubleValue(double value);
        // DECLOAT(16)
        const FB_DEC16* getDecFloat16ValuePtr() const;
        FB_DEC16* getDecFloat16ValuePtr();
        FB_DEC16 getDecFloat16Value() const;
        void setDecFloat16Value(const FB_DEC16& value);
        // DECLOAT(34)
        const FB_DEC34* getDecFloat34ValuePtr() const;
        FB_DEC34* getDecFloat34ValuePtr();
        FB_DEC34 getDecFloat34Value() const;
        void setDecFloat34Value(const FB_DEC34& value);
        // DATE
        ISC_DATE getDateValue() const;
        void setDateValue(ISC_DATE value);
        // TIME
        ISC_TIME getTimeValue() const;
        void setTimeValue(ISC_TIME value);
        // TIMESTAMP
        const ISC_TIMESTAMP* getTimestampPtr() const;
        ISC_TIMESTAMP* getTimestampPtr();
        ISC_TIMESTAMP getTimestampValue() const;
        void setTimestampValue(const ISC_TIMESTAMP& value);
        // CHAR(N)
        const char* getCharPtr() const;
        char* getCharPtr();
        // BINARY(N)
        const unsigned char* getBinaryPtr() const;
        unsigned char* getBinaryPtr();
        // VARCHAR(N)
        const VaryStr* getVaryPtr() const;
        VaryStr* getVaryPtr();
        // VARBINARY(N)
        const VaryBinary* getVarBinaryPtr() const;
        VaryBinary* getVarBinaryPtr();
        // CHAR(N), VARCHAR(N)
        std::string getStringValue() const;
        void setStringValue(std::string_view value);
        // ISC_QUAD (BLOB)
        const ISC_QUAD* getQuadValuePtr() const;
        ISC_QUAD* getQuadValuePtr();
    private:
        unsigned char* buffer;
        Firebird::IMaster* master;
    };

	class FbMessage final {
	public:
		explicit FbMessage(Firebird::ThrowStatusWrapper* status, Firebird::IMaster* master, Firebird::IMessageMetadata* meta);

        void* data() noexcept { return buffer.data(); }
        size_t size() const noexcept { return buffer.size(); }

        size_t count() const noexcept { return fields.size(); }
        sqlda& getSqlda(size_t index);
        const sqlda& getSqlda(size_t index) const;

        sqlda& operator[](size_t index);
        const sqlda& operator[](size_t index) const;
	private:
		std::vector<sqlda> fields;
		std::vector<unsigned char> buffer;
	};

}