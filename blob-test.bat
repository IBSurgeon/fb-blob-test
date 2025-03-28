@echo off
set FB_HOST=localhost
set FB_PORT=3055
set FB_USER=SYSDBA
set FB_PASSWORD=masterkey
set DATABASE=blob_test
set FB_OPT=
set COMPRESSION=1
set MAX_INLINE_BLOB_SIZE=
set LIMIT_ROWS=1000
set AUTO_ILINLE_BLOB=0

set DB_URI=inet://%FB_HOST%

if not [%FB_PORT%]==[] set DB_URI=%DB_URI%:%FB_PORT%

set DB_URI=%DB_URI%/%DATABASE%

if %COMPRESSION% equ 1 set FB_OPT=%FB_OPT% -z
if %AUTO_ILINLE_BLOB% equ 1 set FB_OPT=%FB_OPT% -a

if not [%MAX_INLINE_BLOB_SIZE%]==[] set FB_OPT=%FB_OPT% -i %MAX_INLINE_BLOB_SIZE%
if not [%LIMIT_ROWS%]==[] set FB_OPT=%FB_OPT% -n %LIMIT_ROWS%

echo Database: %DB_URI%
echo Options: %FB_OPT%

fb-blob-test -d %DB_URI% -u %FB_USER% -p %FB_PASSWORD% %FB_OPT%

pause
