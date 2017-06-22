/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.xpack.sql.jdbc.net.client.Cursor;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.util.Version;
import org.elasticsearch.xpack.sql.net.client.util.ObjectUtils;

import static org.elasticsearch.xpack.sql.net.client.util.StringUtils.EMPTY;
import static org.elasticsearch.xpack.sql.net.client.util.StringUtils.hasText;

// Schema Information based on Postgres
// https://www.postgresql.org/docs/9.0/static/information-schema.html

// Currently virtually/synthetic tables are not supported so the client returns
// empty data instead of creating a query
class JdbcDatabaseMetaData implements DatabaseMetaData, JdbcWrapper {

    private final JdbcConnection con;

    JdbcDatabaseMetaData(JdbcConnection con) {
        this.con = con;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return con.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return con.getUserName();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Elasticsearch";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return Version.version();
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Elasticsearch JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return Version.versionMajor() + "." + Version.versionMinor();
    }

    @Override
    public int getDriverMajorVersion() {
        return Version.versionMajor();
    }

    @Override
    public int getDriverMinorVersion() {
        return Version.versionMinor();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        //TODO: is the javadoc accurate
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        // TODO: sync this with the grammar
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        // TODO: sync this with the grammar
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        // TODO: sync this with the grammar
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        // TODO: sync this with the grammar
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        //TODO: add Convert
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "clusterName";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return Connection.TRANSACTION_NONE == level;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    // https://www.postgresql.org/docs/9.0/static/infoschema-routines.html
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return emptySet("ROUTINES",
                     "PROCEDURE_CAT", 
                     "PROCEDURE_SCHEM", 
                     "PROCEDURE_NAME", 
                     "NUM_INPUT_PARAMS", int.class,
                     "NUM_OUTPUT_PARAMS", int.class,
                     "NUM_RESULT_SETS", int.class,
                     "REMARKS",
                     "PROCEDURE_TYPE", short.class,
                     "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return emptySet("PARAMETERS",
                     "PROCEDURE_CAT", 
                     "PROCEDURE_SCHEM", 
                     "PROCEDURE_NAME", 
                     "COLUMN_NAME",
                     "COLUMN_TYPE", short.class,
                     "DATA_TYPE", int.class,
                     "TYPE_NAME",
                     "PRECISION", int.class,
                     "LENGTH", int.class,
                     "SCALE", short.class,
                     "RADIX", short.class,
                     "NULLABLE", short.class,
                     "REMARKS",
                     "COLUMN_DEF",
                     "SQL_DATA_TYPE", int.class,
                     "SQL_DATETIME_SUB", int.class,
                     "CHAR_OCTET_LENGTH", int.class,
                     "ORDINAL_POSITION", int.class,
                     "IS_NULLABLE",
                     "SPECIFIC_NAME");
    }

    // return the cluster name as the catalog (database)
    // helps with the various UIs
    private String defaultCatalog() throws SQLException {
        return con.client.serverInfo().cluster;
    }
    
    private boolean isDefaultCatalog(String catalog) throws SQLException {
        return catalog == null || catalog.equals(EMPTY) || catalog.equals("%") || catalog.equals(defaultCatalog());
    }

    private boolean isDefaultSchema(String schema) {
        return schema == null || schema.equals(EMPTY) || schema.equals("%");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        List<ColumnInfo> info = columnInfo("TABLES",
                                           "TABLE_CAT",
                                           "TABLE_SCHEM",
                                           "TABLE_NAME",
                                           "TABLE_TYPE",
                                           "REMARKS",
                                           "TYPE_CAT",
                                           "TYPE_SCHEM",
                                           "TYPE_NAME",
                                           "SELF_REFERENCING_COL_NAME",
                                           "REF_GENERATION");
 
        // schema and catalogs are not being used, if these are specified return an empty result set
        if (!isDefaultCatalog(catalog) || !isDefaultSchema(schemaPattern)) {
            return emptySet(info);
        }
        
        String cat = defaultCatalog();
        List<String> tables = con.client.metaInfoTables(replaceJdbcWildcardForTables(tableNamePattern));
        Object[][] data = new Object[tables.size()][];
        for (int i = 0; i < data.length; i++) {
            data[i] = new Object[10];
            Object[] row = data[i];

            row[0] = cat;
            row[1] = EMPTY;
            row[2] = tables.get(i);
            row[3] = "TABLE";
            row[4] = EMPTY;
            row[5] = null;
            row[6] = null;
            row[7] = null;
            row[8] = null;
            row[9] = null;
        }
        return memorySet(info, data);
    }

    // this one goes through the ES API
    private static String replaceJdbcWildcardForTables(String tableName) {
        return hasText(tableName) ? tableName.replaceAll("%", "*").replace('_', '?') : tableName;
    }

    // this one gets computed to Pattern matching 
    private static String replaceJdbcWildcardForColumns(String tableName) {
        return hasText(tableName) ? tableName.replaceAll("%", ".*").replace('_', '.') : tableName;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Object[][] data = { { EMPTY, defaultCatalog() } };
        return memorySet(columnInfo("SCHEMATA",
                                    "TABLE_SCHEM", 
                                    "TABLE_CATALOG"), data);
    }
    

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<ColumnInfo> info = columnInfo("SCHEMATA",
                                           "TABLE_SCHEM",
                                           "TABLE_CATALOG");
        if (!isDefaultCatalog(catalog) || !isDefaultSchema(schemaPattern)) {
            return emptySet(info);
        }
        Object[][] data = { { EMPTY, defaultCatalog() } };
        return memorySet(info, data);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Object[][] data = { { defaultCatalog() } };
        return memorySet(columnInfo("CATALOGS",
                                    "TABLE_CAT"), data);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Object[][] data = { { "TABLE" } };
        return memorySet(columnInfo("TABLE_TYPES",
                                    "TABLE_TYPE"), data);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        List<ColumnInfo> info = columnInfo("COLUMNS",
                                           "TABLE_CAT",
                                           "TABLE_SCHEM",
                                           "TABLE_NAME",
                                           "COLUMN_NAME",
                                           "DATA_TYPE", int.class,
                                           "TYPE_NAME",
                                           "COLUMN_SIZE", int.class,
                                           "BUFFER_LENGTH", void.class,
                                           "DECIMAL_DIGITS", int.class,
                                           "NUM_PREC_RADIX", int.class,
                                           "NULLABLE", int.class,
                                           "REMARKS",
                                           "COLUMN_DEF",
                                           "SQL_DATA_TYPE", int.class,
                                           "SQL_DATETIME_SUB", int.class,
                                           "CHAR_OCTET_LENGTH", int.class,
                                           "ORDINAL_POSITION", int.class,
                                           "IS_NULLABLE",
                                           "SCOPE_CATALOG",
                                           "SCOPE_SCHEMA",
                                           "SCOPE_TABLE",
                                           "SOURCE_DATA_TYPE", short.class,
                                           "IS_AUTOINCREMENT",
                                           "IS_GENERATEDCOLUMN");


        // schema and catalogs are not being used, if these are specified return an empty result set
        if (!isDefaultCatalog(catalog) || !isDefaultSchema(schemaPattern)) {
            return emptySet(info);
        }

        String cat = defaultCatalog();
        List<MetaColumnInfo> columns = con.client.metaInfoColumns(replaceJdbcWildcardForTables(tableNamePattern), replaceJdbcWildcardForColumns(columnNamePattern));
        Object[][] data = new Object[columns.size()][];
        for (int i = 0; i < data.length; i++) {
            data[i] = new Object[24];
            Object[] row = data[i];
            MetaColumnInfo col = columns.get(i);
            
            row[ 0] = cat;
            row[ 1] = EMPTY;
            row[ 2] = col.table;
            row[ 3] = col.name;
            row[ 4] = col.type;
            row[ 5] = JdbcUtils.nameOf(col.type);
            row[ 6] = col.position;
            row[ 7] = null;
            row[ 8] = null;
            row[ 9] = 10;
            row[10] = columnNullableUnknown;
            row[11] = null;
            row[12] = null;
            row[13] = null;
            row[14] = null;
            row[15] = null;
            row[16] = col.position;
            row[17] = EMPTY;
            row[18] = null;
            row[19] = null;
            row[20] = null;
            row[21] = null;
            row[22] = EMPTY;
            row[23] = EMPTY;
        }
        return memorySet(info, data);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Privileges not supported");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Privileges not supported");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Row identifiers not supported");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("Version column not supported yet");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("Primary keys not supported");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("Imported keys not supported");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("Exported keys not supported");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Cross reference not supported");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return emptySet("TYPE_INFO",
                     "TYPE_NAME",
                     "DATA_TYPE", int.class,
                     "PRECISION", int.class,
                     "LITERAL_PREFIX",
                     "LITERAL_SUFFIX",
                     "CREATE_PARAMS",
                     "NULLABLE", short.class,
                     "CASE_SENSITIVE", boolean.class,
                     "SEARCHABLE", short.class,
                     "UNSIGNED_ATTRIBUTE", boolean.class,
                     "FIXED_PREC_SCALE", boolean.class,
                     "AUTO_INCREMENT", boolean.class,
                     "LOCAL_TYPE_NAME",
                     "MINIMUM_SCALE", short.class,
                     "MAXIMUM_SCALE", short.class,
                     "SQL_DATA_TYPE", int.class,
                     "SQL_DATETIME_SUB", int.class,
                     "NUM_PREC_RADIX", int.class
                     );
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException("Indicies not supported");
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type && ResultSet.CONCUR_READ_ONLY == concurrency;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return emptySet("USER_DEFINED_TYPES",
                    "TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "CLASS_NAME",
                    "DATA_TYPE", int.class,
                    "REMARKS",
                    "BASE_TYPE", short.class);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return con;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptySet("SUPER_TYPES",
                     "TYPE_CAT",
                     "TYPE_SCHEM",
                     "TYPE_NAME",
                     "SUPERTYPE_CAT",
                     "SUPERTYPE_SCHEM",
                     "SUPERTYPE_NAME",
                     "BASE_TYPE");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptySet("SUPER_TABLES",
                     "TABLE_CAT",
                     "TABLE_SCHEM",
                     "TABLE_NAME",
                     "SUPERTABLE_NAME");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return emptySet("ATTRIBUTES",
                     "TYPE_CAT",
                     "TYPE_SCHEM",
                     "TYPE_NAME",
                     "ATTR_NAME",
                     "DATA_TYPE", int.class,
                     "ATTR_TYPE_NAME",
                     "ATTR_SIZE", int.class,
                     "DECIMAL_DIGITS", int.class,
                     "NUM_PREC_RADIX", int.class,
                     "NULLABLE", int.class,
                     "REMARKS",
                     "ATTR_DEF",
                     "SQL_DATA_TYPE", int.class,
                     "SQL_DATETIME_SUB", int.class,
                     "CHAR_OCTET_LENGTH", int.class,
                     "ORDINAL_POSITION", int.class,
                     "IS_NULLABLE",
                     "SCOPE_CATALOG",
                     "SCOPE_SCHEMA",
                     "SCOPE_TABLE",
                     "SOURCE_DATA_TYPE", short.class);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return con.esInfoMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return con.esInfoMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return JdbcDriver.jdbcMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return JdbcDriver.jdbcMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new UnsupportedOperationException("Client info not implemented yet");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return emptySet("FUNCTIONS",
                     "FUNCTION_CAT",
                     "FUNCTION_SCHEM",
                     "FUNCTION_NAME",
                     "REMARKS",
                     "FUNCTION_TYPE", short.class,
                     "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return emptySet("FUNCTION_COLUMNS",
                     "FUNCTION_CAT",
                     "FUNCTION_SCHEM",
                     "FUNCTION_NAME",
                     "COLUMN_NAME",
                     "DATA_TYPE", int.class,
                     "TYPE_NAME",
                     "PRECISION", int.class,
                     "LENGTH", int.class,
                     "SCALE", short.class,
                     "RADIX", short.class,
                     "NULLABLE", short.class,
                     "REMARKS",
                     "CHAR_OCTET_LENGTH", int.class,
                     "ORDINAL_POSITION", int.class,
                     "IS_NULLABLE",
                     "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return emptySet("PSEUDO_COLUMNS",
                     "TABLE_CAT",
                     "TABLE_SCHEM",
                     "TABLE_NAME",
                     "COLUMN_NAME",
                     "DATA_TYPE", int.class,
                     "COLUMN_SIZE", int.class,
                     "DECIMAL_DIGITS", int.class,
                     "NUM_PREC_RADIX", int.class,
                     "REMARKS",
                     "COLUMN_USAGE",
                     "IS_NULLABLE");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    private static List<ColumnInfo> columnInfo(String tableName, Object... cols) {
        List<ColumnInfo> columns = new ArrayList<>();

        for (int i = 0; i < cols.length; i++) {
            Object obj = cols[i];
            if (obj instanceof String) {
                String name = obj.toString();
                int type = Types.VARCHAR;
                if (i + 1 < cols.length) {
                    // check if the next item it's a type
                    if (cols[i + 1] instanceof Class) {
                        type = JdbcUtils.fromClass((Class<?>) cols[i + 1]);
                        i++;
                    }
                    // it's not, use the default and move on
                }
                columns.add(new ColumnInfo(name, type, tableName, "INFORMATION_SCHEMA", EMPTY, EMPTY));
            }
            else {
                throw new JdbcException("Invalid metadata schema definition");
            }
        }
        return columns;
    }

    private static ResultSet emptySet(String tableName, Object... cols) {
        return new JdbcResultSet(null, new InMemoryCursor(columnInfo(tableName, cols), null));
    }

    private static ResultSet emptySet(List<ColumnInfo> columns) {
        return memorySet(columns, null);
    }

    private static ResultSet memorySet(List<ColumnInfo> columns, Object[][] data) {
        return new JdbcResultSet(null, new InMemoryCursor(columns, data));
    }

    static class InMemoryCursor implements Cursor {

        private final List<ColumnInfo> columns;
        private final Object[][] data;

        private int row = -1;

        InMemoryCursor(List<ColumnInfo> info, Object[][] data) {
            this.columns = info;
            this.data = data;
        }

        @Override
        public List<ColumnInfo> columns() {
            return columns;
        }

        @Override
        public boolean next() {
            if (!ObjectUtils.isEmpty(data) && row < data.length - 1) {
                row++;
                return true;
            }
            return false;
        }

        @Override
        public Object column(int column) {
            return data[row][column];
        }
    }
}