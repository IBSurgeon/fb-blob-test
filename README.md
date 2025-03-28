# Test BLOB transfer over the wire

## Preparing the database

Let's create a table to store the files. For example, I loaded the contents of the `lucene_udr` library source code (https://github.com/IBSurgeon/lucene_udr) into this table. Source code files can be short or long, so they are ideal for demonstrating BLOB transfer over the wire.

```sql
CREATE TABLE BLOB_SAMPLE (
    ID         BIGINT GENERATED BY DEFAULT AS IDENTITY,
    FILE_NAME  VARCHAR(255) CHARACTER SET UTF8 NOT NULL,
    CONTENT    BLOB SUB_TYPE TEXT CHARACTER SET UTF8
);

ALTER TABLE BLOB_SAMPLE ADD PRIMARY KEY (ID);
ALTER TABLE BLOB_SAMPLE ADD UNIQUE (FILE_NAME);
```

Since the project is not large, the number of source code files in it is not as large as we would like. To make the testing results more visual in numbers, we will increase the number of BLOB records to 10,000. To do this, we will create a separate table `BLOB_TEST` with the following structure:

```sql
RECREATE TABLE BLOB_TEST (
    ID             BIGINT GENERATED BY DEFAULT AS IDENTITY,
    SHORT_CONTENT  VARCHAR(8191) CHARACTER SET UTF8,
    CONTENT        BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    SHORT_BLOB     BOOLEAN DEFAULT FALSE NOT NULL,
    CONSTRAINT PK_BLOB_TEST PRIMARY KEY (ID)
);
```

The following script is used to fill the test table:

```sql
SET TERM ^;

EXECUTE BLOCK
AS
DECLARE I INTEGER = 0;
DECLARE IS_SHORT BOOLEAN;
BEGIN
  WHILE (TRUE) DO
  BEGIN
    FOR
      SELECT
        ID,
        FILE_NAME,
        CONTENT,
        CHAR_LENGTH(CONTENT) AS CH_L,
        OCTET_LENGTH(CONTENT) AS OC_L
      FROM BLOB_SAMPLE
      ORDER BY FILE_NAME
      AS CURSOR C
    DO
    BEGIN
      I = I + 1;
      -- The BLOB is placed into a string variable that is 8191 characters long.
      IS_SHORT = (C.CH_L < 8191);

      INSERT INTO BLOB_TEST (
        SHORT_CONTENT,
        CONTENT,
        SHORT_BLOB
      )
      VALUES (
        IIF(:IS_SHORT, :C.CONTENT, NULL), -- if BLOB is short we write it in VARCHAR field
        :C.CONTENT,
        :IS_SHORT
      );
      IF (I = 10000) THEN EXIT;
    END
  END
END^

SET TERM ;^

COMMIT;
```

## Description fb-blob-test

To get help about application switches, enter the command:

```bash
fb-blob-test -h
```

The following help will be displayed:

```
Usage fb-blob-test [<database>] <options>
General options:
    -h [ --help ]                        Show help

Database options:
    -d [ --database ] connection_string  Database connection string
    -u [ --username ] user               User name
    -p [ --password ] password           Password
    -c [ --charset ] charset             Character set, default UTF8
    -n [ --limit-rows ] value            Limit of rows
    -i [ --max-inline-blob-size ] value  Maximum inline blob size, default 65535
    -z [ --compress ]                    Wire compression, default False
    -a [ --auto-blob-inline ]            Set optimal maximum inline blob size for each statement
```

Example of use:

```bash
fb-blob-test -d inet://localhost/blob_test -u SYSDBA -p masterkey -z
```

## Example of output

```
===== Test of BLOBs transmission over the network =====

Firebird server version
Firebird/Windows/AMD/Intel/x64 (access method), version "WI-V5.0.3.1627 Firebird 5.0 5e33dc5"
Firebird/Windows/AMD/Intel/x64 (remote server), version "WI-V5.0.3.1627 Firebird 5.0 5e33dc5/tcp (station9)/P19:CZ"
Firebird/Windows/AMD/Intel/x64 (remote interface), version "WI-V5.0.3.1627 Firebird 5.0 5e33dc5/tcp (station9)/P19:CZ"
on disk structure version 13.1

** Warming up the cache **
------------------------------------------------------------------------------------
SQL:

SELECT
  MAX(CHAR_LENGTH(CONTENT)) AS MAX_CHAR_LENGTH
FROM BLOB_TEST

Elapsed time: 494ms
Max char length: 77743

** Test read short BLOBs **
------------------------------------------------------------------------------------
SQL:

SELECT
  ID,
  CONTENT
FROM BLOB_TEST
WHERE SHORT_BLOB IS TRUE

MaxInlineBlobSize = 65535
Elapsed time: 569ms
Max id: 10000
Record count: 5884
Content size: 19810796 bytes
Wire logical statistics:
  send packets = 24
  recv packets = 11792
  send bytes = 516
  recv bytes = 20325368
Wire physical statistics:
  send packets = 23
  recv packets = 422
  send bytes = 201
  recv bytes = 3368040
  roundtrips = 23

** Test read VARCHAR(8191) **
------------------------------------------------------------------------------------
SQL:

SELECT
  ID,
  SHORT_CONTENT
FROM BLOB_TEST
WHERE SHORT_BLOB IS TRUE

Elapsed time: 565ms
Max id: 10000
Record count: 5884
Content size: 19810796 bytes
Wire logical statistics:
  send packets = 192
  recv packets = 6076
  send bytes = 3876
  recv bytes = 19987304
Wire physical statistics:
  send packets = 191
  recv packets = 522
  send bytes = 1543
  recv bytes = 3257193
  roundtrips = 191

** Test read all BLOBs **
------------------------------------------------------------------------------------
SQL:

SELECT
  ID,
  CONTENT
FROM BLOB_TEST

MaxInlineBlobSize = 65535
Elapsed time: 3508ms
Max id: 10000
Record count: 10000
Content size: 126392558 bytes
Wire logical statistics:
  send packets = 1854
  recv packets = 21560
  send bytes = 32424
  recv bytes = 127328028
Wire physical statistics:
  send packets = 972
  recv packets = 3640
  send bytes = 9171
  recv bytes = 18590181
  roundtrips = 883

** Test read mixed BLOBs and VARCHARs **
------------------------------------------------------------------------------------
SQL:

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

MaxInlineBlobSize = 65535
Elapsed time: 4239ms
Max id: 10000
Record count: 10000
Content size: 126392558 bytes
Wire logical statistics:
  send packets = 2087
  recv packets = 15909
  send bytes = 37092
  recv bytes = 126990724
Wire physical statistics:
  send packets = 1205
  recv packets = 3819
  send bytes = 10942
  recv bytes = 18616305
  roundtrips = 1074

** Test read mixed BLOBs and VARCHARs with optimize **
------------------------------------------------------------------------------------
SQL:

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

MaxInlineBlobSize = 65535
Elapsed time: 3458ms
Max id: 10000
Record count: 10000
Content size: 126392558 bytes
Wire logical statistics:
  send packets = 2087
  recv packets = 15909
  send bytes = 37092
  recv bytes = 126990724
Wire physical statistics:
  send packets = 1205
  recv packets = 3736
  send bytes = 10936
  recv bytes = 18616305
  roundtrips = 1074

** Test read only BLOB IDs **
------------------------------------------------------------------------------------
SQL:

SELECT
  ID,
  CONTENT
FROM BLOB_TEST

MaxInlineBlobSize = 65535
Elapsed time: 2266ms
Max id: 10000
Record count: 10000
Wire logical statistics:
  send packets = 91
  recv packets = 19797
  send bytes = 1856
  recv bytes = 104384300
Wire physical statistics:
  send packets = 90
  recv packets = 1701
  send bytes = 755
  recv bytes = 13489405
  roundtrips = 90
```

## Download the utility and database for testing

https://github.com/IBSurgeon/fb-blob-test/releases/download/1.0/fb-blob-test.zip

## Download article

* PDF - [fb_wire_blobs_en.pdf](https://github.com/IBSurgeon/fb-blob-test/releases/download/1.0/fb_wire_blobs_en.pdf)
* HTML - [fb_wire_blobs_en-html.zip](https://github.com/IBSurgeon/fb-blob-test/releases/download/1.0/fb_wire_blobs_en-html.zip)
