@echo off
set FB_HOST=localhost
set FB_PORT=3055
set FB_USER=SYSDBA
set FB_PASSWORD=masterkey
set DATABASE=blob_test
set FB_OPT=
set COMPRESSION=1
set MAX_INLINE_BLOB_SIZE=

set DB_URI=inet://%FB_HOST%

if not [%FB_PORT%]==[] set DB_URI=%DB_URI%:%FB_PORT%

set DB_URI=%DB_URI%/%DATABASE%

if %COMPRESSION% equ 1 set FB_OPT=%FB_OPT% -z

if not [%MAX_INLINE_BLOB_SIZE%]==[] set FB_OPT=%FB_OPT% -i %MAX_INLINE_BLOB_SIZE%

echo Database: %DB_URI%
echo Options: %FB_OPT%

fbcpp-inline-blob -d %DB_URI% -u %FB_USER% -p %FB_PASSWORD% %FB_OPT%

pause
