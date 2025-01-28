/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.apache.lucene.util.Counter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.index.IndexCompatibility;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.ListCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;
import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;
import static org.elasticsearch.xpack.ql.type.DataTypes.BINARY;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.ql.type.DataTypes.isString;
import static org.elasticsearch.xpack.ql.util.StringUtils.isQualified;
import static org.elasticsearch.xpack.ql.util.StringUtils.splitQualifiedIndex;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.displaySize;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlDataType;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlDateTimeSub;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlRadix;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.sqlType;

/**
 * System command designed to be used by JDBC / ODBC for column metadata, such as
 * {@link DatabaseMetaData#getColumns(String, String, String, String)}.
 */
public class SysColumns extends Command {

    private final String catalog;
    private final String index;
    private final LikePattern pattern;
    private final LikePattern columnPattern;

    public SysColumns(Source source, String catalog, String index, LikePattern pattern, LikePattern columnPattern) {
        super(source);
        this.catalog = catalog;
        this.index = index;
        this.pattern = pattern;
        this.columnPattern = columnPattern;
    }

    @Override
    protected NodeInfo<SysColumns> info() {
        return NodeInfo.create(this, SysColumns::new, catalog, index, pattern, columnPattern);
    }

    @Override
    public List<Attribute> output() {
        return output(false);
    }

    private List<Attribute> output(boolean odbcCompatible) {
        // https://github.com/elastic/elasticsearch/issues/35376
        // ODBC expects some fields as SHORT while JDBC as Integer
        // which causes conversion issues and CCE
        DataType clientBasedType = odbcCompatible ? SHORT : INTEGER;
        return asList(
            keyword("TABLE_CAT"),
            keyword("TABLE_SCHEM"),
            keyword("TABLE_NAME"),
            keyword("COLUMN_NAME"),
            field("DATA_TYPE", clientBasedType),
            keyword("TYPE_NAME"),
            field("COLUMN_SIZE", INTEGER),
            field("BUFFER_LENGTH", INTEGER),
            field("DECIMAL_DIGITS", clientBasedType),
            field("NUM_PREC_RADIX", clientBasedType),
            field("NULLABLE", clientBasedType),
            keyword("REMARKS"),
            keyword("COLUMN_DEF"),
            field("SQL_DATA_TYPE", clientBasedType),
            field("SQL_DATETIME_SUB", clientBasedType),
            field("CHAR_OCTET_LENGTH", INTEGER),
            field("ORDINAL_POSITION", INTEGER),
            keyword("IS_NULLABLE"),
            // JDBC specific
            keyword("SCOPE_CATALOG"),
            keyword("SCOPE_SCHEMA"),
            keyword("SCOPE_TABLE"),
            field("SOURCE_DATA_TYPE", SHORT),
            keyword("IS_AUTOINCREMENT"),
            keyword("IS_GENERATEDCOLUMN")
        );
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        Mode mode = session.configuration().mode();
        List<Attribute> output = output(mode == Mode.ODBC);
        String cluster = session.indexResolver().clusterName();

        // save original index name (as the pattern can contain special chars)
        String indexName = index != null
            ? index
            : (pattern != null ? StringUtils.likeToUnescaped(pattern.pattern(), pattern.escape()) : "");
        String idx = index != null ? index : (pattern != null ? pattern.asIndexNameWildcard() : "*");
        String regex = pattern != null ? pattern.asJavaRegex() : null;

        Pattern columnMatcher = columnPattern != null ? Pattern.compile(columnPattern.asJavaRegex()) : null;
        boolean includeFrozen = session.configuration().includeFrozen();

        // disallow double catalog specification, like: SYS COLUMNS CATALOG 'catA' TABLE LIKE 'catB:index_expression'
        if (isQualified(idx)) {
            throw new IllegalArgumentException(
                "illegal character ["
                    + REMOTE_CLUSTER_INDEX_SEPARATOR
                    + "] (the catalog delimiter) "
                    + "found in the table expression ["
                    + idx
                    + "]"
            );
        }

        String indexPattern = idx;
        String tableCat;
        if (Strings.hasText(catalog)) {
            // SYS COLUMNS's catalog "cannot contain a string search pattern" (by xDBC specs) -> it must match local or a remote cluster.
            // Require an exact match for local cluster, since a pattern might match local and remote clusters, which cannot be searched.
            if (catalog.equals(cluster) == false) {
                indexPattern = buildRemoteIndexName(catalog, idx);
            }
            tableCat = catalog;
        } else {
            tableCat = cluster;
        }

        SqlVersion version = session.configuration().version();
        // special case for '%' (translated to *)
        if ("*".equals(idx)) {
            session.indexResolver()
                .resolveAsSeparateMappings(
                    indexPattern,
                    regex,
                    includeFrozen,
                    emptyMap(),
                    listener.delegateFailureAndWrap((delegate, esIndices) -> {
                        List<List<?>> rows = new ArrayList<>();
                        for (EsIndex esIndex : esIndices) {
                            IndexCompatibility.compatible(esIndex, version);
                            fillInRows(tableCat, esIndex.name(), esIndex.mapping(), null, rows, columnMatcher, mode);
                        }
                        delegate.onResponse(ListCursor.of(Rows.schema(output), rows, session.configuration().pageSize()));
                    })
                );
        }
        // otherwise use a merged mapping
        else {
            session.indexResolver()
                .resolveAsMergedMapping(
                    indexPattern,
                    IndexResolver.ALL_FIELDS,
                    includeFrozen,
                    emptyMap(),
                    listener.delegateFailureAndWrap((delegate, r) -> {
                        List<List<?>> rows = new ArrayList<>();
                        // populate the data only when a target is found
                        if (r.isValid()) {
                            fillInRows(
                                tableCat,
                                indexName,
                                IndexCompatibility.compatible(r, version).get().mapping(),
                                null,
                                rows,
                                columnMatcher,
                                mode
                            );
                        }
                        delegate.onResponse(ListCursor.of(Rows.schema(output), rows, session.configuration().pageSize()));
                    })
                );
        }
    }

    static void fillInRows(
        String clusterName,
        String indexName,
        Map<String, EsField> mapping,
        String prefix,
        List<List<?>> rows,
        Pattern columnMatcher,
        Mode mode
    ) {
        fillInRows(clusterName, splitQualifiedIndex(indexName).v2(), mapping, prefix, rows, columnMatcher, Counter.newCounter(), mode);
    }

    private static void fillInRows(
        String clusterName,
        String indexName,
        Map<String, EsField> mapping,
        String prefix,
        List<List<?>> rows,
        Pattern columnMatcher,
        Counter position,
        Mode mode
    ) {
        boolean isOdbcClient = mode == Mode.ODBC;
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            position.addAndGet(1); // JDBC is 1-based so we start with 1 here

            String name = entry.getKey();
            name = prefix != null ? prefix + "." + name : name;
            EsField field = entry.getValue();
            DataType type = field.getDataType();

            // skip the nested, object and unsupported types
            if (isPrimitive(type)) {
                if (columnMatcher == null || columnMatcher.matcher(name).matches()) {
                    rows.add(
                        asList(
                            clusterName,
                            // schema is not supported
                            null,
                            indexName,
                            name,
                            odbcCompatible(sqlType(type).getVendorTypeNumber(), isOdbcClient),
                            type.toString(),
                            displaySize(type),
                            // TODO: is the buffer_length correct?
                            type.size(),
                            // no DECIMAL support
                            null,
                            odbcCompatible(metaSqlRadix(type), isOdbcClient),
                            // everything is nullable
                            odbcCompatible(DatabaseMetaData.columnNullable, isOdbcClient),
                            // no remarks
                            null,
                            // no column def
                            null,
                            // SQL_DATA_TYPE apparently needs to be same as DATA_TYPE except for datetime and interval data types
                            odbcCompatible(metaSqlDataType(type), isOdbcClient),
                            // SQL_DATETIME_SUB ?
                            odbcCompatible(metaSqlDateTimeSub(type), isOdbcClient),
                            // char octet length
                            isString(type) || type == BINARY ? type.size() : null,
                            // position
                            (int) position.get(),
                            "YES",
                            null,
                            null,
                            null,
                            null,
                            "NO",
                            "NO"
                        )
                    );
                }
            }
            // skip nested fields
            if (field.getProperties() != null && type != NESTED) {
                fillInRows(clusterName, indexName, field.getProperties(), name, rows, columnMatcher, position, mode);
            }
        }
    }

    private static Object odbcCompatible(Integer value, boolean isOdbcClient) {
        if (isOdbcClient && value != null) {
            return Short.valueOf(value.shortValue());
        }
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, index, pattern, columnPattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SysColumns other = (SysColumns) obj;
        return Objects.equals(catalog, other.catalog)
            && Objects.equals(index, other.index)
            && Objects.equals(pattern, other.pattern)
            && Objects.equals(columnPattern, other.columnPattern);
    }
}
