CREATE TABLE mock (
  TABLE_SCHEM VARCHAR,
  TABLE_NAME VARCHAR,
  COLUMN_NAME VARCHAR,
  DATA_TYPE INTEGER,
  TYPE_NAME VARCHAR,
  COLUMN_SIZE INTEGER,
  BUFFER_LENGTH INTEGER,
  DECIMAL_DIGITS INTEGER,
  NUM_PREC_RADIX INTEGER,
  NULLABLE INTEGER,
  REMARKS VARCHAR,
  COLUMN_DEF VARCHAR,
  SQL_DATA_TYPE INTEGER,
  SQL_DATETIME_SUB INTEGER,
  CHAR_OCTET_LENGTH INTEGER,
  ORDINAL_POSITION INTEGER,
  IS_NULLABLE VARCHAR,
  SCOPE_CATALOG VARCHAR,
  SCOPE_SCHEMA VARCHAR,
  SCOPE_TABLE VARCHAR,
  SOURCE_DATA_TYPE SMALLINT,
  IS_AUTOINCREMENT VARCHAR,
  IS_GENERATEDCOLUMN VARCHAR
) AS
SELECT null, 'test1', 'name', 12, 'TEXT', 2147483647, 2147483647, null, null,
  1, -- columnNullable
  null, null, 12, 0, 2147483647, 1, 'YES', null, null, null, null, 'NO', 'NO'
FROM DUAL
UNION ALL
SELECT null, 'test1', 'name.keyword', 12, 'KEYWORD', 32766, 2147483647, null, null,
  1, -- columnNullable
  null, null, 12, 0, 2147483647, 2, 'YES', null, null, null, null, 'NO', 'NO'
FROM DUAL
UNION ALL
SELECT null, 'test2', 'date', 93, 'DATETIME', 29, 8, null, null,
  1, -- columnNullable
  null, null, 9, 3, null, 1, 'YES', null, null, null, null, 'NO', 'NO'
FROM DUAL
UNION ALL
SELECT null, 'test2', 'float', 7, 'FLOAT', 15, 4, null, 2,
  1, -- columnNullable
  null, null, 7, 0, null, 2, 'YES', null, null, null, null, 'NO', 'NO'
FROM DUAL
UNION ALL
SELECT null, 'test2', 'number', -5, 'LONG', 20, 8, null, 10,
  1, -- columnNullable
  null, null, -5, 0, null, 3, 'YES', null, null, null, null, 'NO', 'NO'
FROM DUAL
;
