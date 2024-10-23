/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.action.Protocol;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.index.IndexCompatibility;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersions;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.TestUtils.UTC;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.sql.analysis.analyzer.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.sql.index.VersionCompatibilityChecks.isTypeSupportedInVersion;
import static org.elasticsearch.xpack.sql.proto.Mode.isDriver;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;
import static org.elasticsearch.xpack.sql.util.SqlVersionUtils.UNSIGNED_LONG_TEST_VERSIONS;
import static org.elasticsearch.xpack.sql.util.SqlVersionUtils.VERSION_FIELD_TEST_VERSIONS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysColumnsTests extends ESTestCase {

    private static final String CLUSTER_NAME = "cluster";
    private static final Map<String, EsField> MAPPING1 = loadMapping("mapping-multi-field-with-nested.json", true);
    private static final Map<String, EsField> MAPPING2 = loadMapping("mapping-multi-field-variation.json", true);
    private static final int FIELD_COUNT1 = 20;
    private static final int FIELD_COUNT2 = 19;

    private final SqlParser parser = new SqlParser();

    private void sysColumnsInMode(Mode mode) {
        Class<? extends Number> typeClass = mode == Mode.ODBC ? Short.class : Integer.class;
        List<List<?>> rows = new ArrayList<>();
        SysColumns.fillInRows("test", "org/elasticsearch/xpack/sql/index", MAPPING2, null, rows, null, mode);
        assertEquals(FIELD_COUNT2, rows.size());
        assertEquals(24, rows.get(0).size());

        int index = 0;
        List<?> row = rows.get(index++);
        assertDriverType("bool", Types.BOOLEAN, false, 1, 1, typeClass, row);

        row = rows.get(index++);
        assertDriverType("int", Types.INTEGER, true, 11, 4, typeClass, row);

        row = rows.get(index++);
        assertDriverType("unsigned_long", Types.NUMERIC, true, 20, Long.BYTES, typeClass, row);

        row = rows.get(index++);
        assertDriverType("float", Types.REAL, true, 15, 4, typeClass, row);

        row = rows.get(index++);
        assertDriverType("text", Types.VARCHAR, false, Integer.MAX_VALUE, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("keyword", Types.VARCHAR, false, Short.MAX_VALUE - 1, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("date", Types.TIMESTAMP, false, 34, 8, typeClass, row);

        row = rows.get(index++);
        assertDriverType("date_nanos", Types.TIMESTAMP, false, 34, 8, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.dotted.field", Types.VARCHAR, false, Short.MAX_VALUE - 1, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.string", Types.VARCHAR, false, Integer.MAX_VALUE, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.string.normalized", Types.VARCHAR, false, Short.MAX_VALUE - 1, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.string.typical", Types.VARCHAR, false, Short.MAX_VALUE - 1, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.ambiguous", Types.VARCHAR, false, Integer.MAX_VALUE, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.ambiguous.one", Types.VARCHAR, false, Short.MAX_VALUE - 1, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.ambiguous.two", Types.VARCHAR, false, Short.MAX_VALUE - 1, Integer.MAX_VALUE, typeClass, row);

        row = rows.get(index++);
        assertDriverType("some.ambiguous.normalized", Types.VARCHAR, false, Short.MAX_VALUE - 1, Integer.MAX_VALUE, typeClass, row);
    }

    public void testSysColumnsInJdbcMode() {
        sysColumnsInMode(Mode.JDBC);
    }

    public void testSysColumnsInOdbcMode() {
        sysColumnsInMode(Mode.ODBC);
    }

    public void testInNonDriverMode() {
        for (Mode mode : Mode.values()) {
            if (isDriver(mode) == false) {
                sysColumnsInMode(mode);
            }
        }
    }

    public void testUnsignedLongFiltering() {
        for (Mode mode : List.of(Mode.JDBC, Mode.ODBC)) {
            for (SqlVersion version : UNSIGNED_LONG_TEST_VERSIONS) {
                List<List<?>> rows = new ArrayList<>();
                // mapping's mutated by IndexCompatibility.compatible, needs to stay in the loop
                Map<String, EsField> mapping = loadMapping("mapping-multi-field-variation.json", true);
                SysColumns.fillInRows(
                    "test",
                    "org/elasticsearch/xpack/sql/index",
                    IndexCompatibility.compatible(mapping, version),
                    null,
                    rows,
                    null,
                    mode
                );
                List<String> types = rows.stream().map(row -> name(row).toString()).collect(Collectors.toList());
                assertEquals(
                    isTypeSupportedInVersion(UNSIGNED_LONG, version),
                    types.contains(UNSIGNED_LONG.toString().toLowerCase(Locale.ROOT))
                );
            }
        }
    }

    public void testVersionTypeFiltering() {
        for (Mode mode : List.of(Mode.JDBC, Mode.ODBC)) {
            for (SqlVersion version : VERSION_FIELD_TEST_VERSIONS) {
                List<List<?>> rows = new ArrayList<>();
                // mapping's mutated by IndexCompatibility.compatible, needs to stay in the loop
                Map<String, EsField> mapping = loadMapping("mapping-multi-field-variation.json", true);
                SysColumns.fillInRows(
                    "test",
                    "org/elasticsearch/xpack/sql/index",
                    IndexCompatibility.compatible(mapping, version),
                    null,
                    rows,
                    null,
                    mode
                );
                List<String> types = rows.stream().map(row -> name(row).toString()).collect(Collectors.toList());
                assertEquals(isTypeSupportedInVersion(VERSION, version), types.contains(VERSION.toString().toLowerCase(Locale.ROOT)));
            }
        }
    }

    private static Object name(List<?> list) {
        return list.get(3);
    }

    private static Object sqlType(List<?> list) {
        return list.get(4);
    }

    private static Object precision(List<?> list) {
        return list.get(6);
    }

    private static Object bufferLength(List<?> list) {
        return list.get(7);
    }

    private static Object decimalPrecision(List<?> list) {
        return list.get(8);
    }

    private static Object radix(List<?> list) {
        return list.get(9);
    }

    private static Object nullable(List<?> list) {
        return list.get(10);
    }

    private static Object sqlDataType(List<?> list) {
        return list.get(13);
    }

    private static Object sqlDataTypeSub(List<?> list) {
        return list.get(14);
    }

    public void testSysColumnsNoArg() {
        executeCommand("SYS COLUMNS", emptyList(), r -> {
            assertEquals(FIELD_COUNT1, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            // no index specified
            assertEquals("test", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            // no index specified
            assertEquals("test", r.column(2));
            assertEquals("int", r.column(3));
        }, MAPPING1);
    }

    public void testSysColumnsWithCatalogWildcard() {
        executeCommand("SYS COLUMNS CATALOG '" + CLUSTER_NAME + "' TABLE LIKE 'test' LIKE '%'", emptyList(), r -> {
            assertEquals(FIELD_COUNT1, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("int", r.column(3));
        }, MAPPING1);
    }

    public void testSysColumnsWithMissingCatalog() {
        executeCommand("SYS COLUMNS TABLE LIKE 'test' LIKE '%'", emptyList(), r -> {
            assertEquals(FIELD_COUNT1, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("int", r.column(3));
        }, MAPPING1);
    }

    public void testSysColumnsWithNullCatalog() {
        executeCommand("SYS COLUMNS CATALOG ? TABLE LIKE 'test' LIKE '%'", singletonList(new SqlTypedParamValue("keyword", null)), r -> {
            assertEquals(FIELD_COUNT1, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("int", r.column(3));
        }, MAPPING1);
    }

    public void testSysColumnsTypesInOdbcMode() {
        executeCommand("SYS COLUMNS", emptyList(), Mode.ODBC, SysColumnsTests::checkOdbcShortTypes, MAPPING1);
        executeCommand("SYS COLUMNS TABLE LIKE 'test'", emptyList(), Mode.ODBC, SysColumnsTests::checkOdbcShortTypes, MAPPING1);
    }

    public void testSysColumnsPaginationInOdbcMode() {
        assertEquals(FIELD_COUNT1, executeCommandInOdbcModeAndCountRows("SYS COLUMNS"));
        assertEquals(FIELD_COUNT1, executeCommandInOdbcModeAndCountRows("SYS COLUMNS TABLE LIKE 'test'"));
    }

    private int executeCommandInOdbcModeAndCountRows(String sql) {
        final SqlConfiguration config = new SqlConfiguration(
            DateUtils.UTC,
            null,
            randomIntBetween(1, 15),
            Protocol.REQUEST_TIMEOUT,
            Protocol.PAGE_TIMEOUT,
            null,
            null,
            Mode.ODBC,
            null,
            SqlVersions.SERVER_COMPAT_VERSION,
            null,
            null,
            false,
            false,
            null,
            null,
            false
        );
        Tuple<Command, SqlSession> tuple = sql(sql, emptyList(), config, MAPPING1);

        int[] rowCount = { 0 };
        tuple.v1().execute(tuple.v2(), new ActionListener<>() {
            @Override
            public void onResponse(Cursor.Page page) {
                Cursor c = page.next();
                rowCount[0] += page.rowSet().size();
                if (c != Cursor.EMPTY) {
                    c.nextPage(config, null, this);
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.getMessage());
            }
        });
        return rowCount[0];
    }

    private void executeCommand(
        String sql,
        List<SqlTypedParamValue> params,
        Mode mode,
        Consumer<SchemaRowSet> consumer,
        Map<String, EsField> mapping
    ) {
        final SqlConfiguration config = new SqlConfiguration(
            DateUtils.UTC,
            null,
            Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT,
            Protocol.PAGE_TIMEOUT,
            null,
            null,
            mode,
            null,
            SqlVersions.SERVER_COMPAT_VERSION,
            null,
            null,
            false,
            false,
            null,
            null,
            false
        );
        Tuple<Command, SqlSession> tuple = sql(sql, params, config, mapping);

        tuple.v1().execute(tuple.v2(), ActionTestUtils.assertNoFailureListener(p -> consumer.accept((SchemaRowSet) p.rowSet())));
    }

    private void executeCommand(
        String sql,
        List<SqlTypedParamValue> params,
        Consumer<SchemaRowSet> consumer,
        Map<String, EsField> mapping
    ) {
        executeCommand(sql, params, Mode.PLAIN, consumer, mapping);
    }

    @SuppressWarnings({ "unchecked" })
    private Tuple<Command, SqlSession> sql(
        String sql,
        List<SqlTypedParamValue> params,
        SqlConfiguration config,
        Map<String, EsField> mapping
    ) {
        EsIndex test = new EsIndex("test", mapping);
        Analyzer analyzer = analyzer(config, IndexResolution.valid(test));
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql, params, UTC), true);

        IndexResolver resolver = mock(IndexResolver.class);
        when(resolver.clusterName()).thenReturn(CLUSTER_NAME);
        when(resolver.remoteClusters()).thenReturn(Set.of(CLUSTER_NAME));
        doAnswer(invocation -> {
            ((ActionListener<IndexResolution>) invocation.getArguments()[4]).onResponse(IndexResolution.valid(test));
            return Void.TYPE;
        }).when(resolver).resolveAsMergedMapping(any(), eq(IndexResolver.ALL_FIELDS), anyBoolean(), any(), any());
        doAnswer(invocation -> {
            ((ActionListener<List<EsIndex>>) invocation.getArguments()[4]).onResponse(singletonList(test));
            return Void.TYPE;
        }).when(resolver).resolveAsSeparateMappings(any(), any(), anyBoolean(), any(), any());

        SqlSession session = new SqlSession(config, null, null, resolver, null, null, null, null, null);
        return new Tuple<>(cmd, session);
    }

    private static void checkOdbcShortTypes(SchemaRowSet r) {
        assertEquals(FIELD_COUNT1, r.size());
        // https://github.com/elastic/elasticsearch/issues/35376
        // cols that need to be of short type: DATA_TYPE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, SQL_DATA_TYPE, SQL_DATETIME_SUB
        List<Integer> cols = Arrays.asList(4, 8, 9, 10, 13, 14);
        for (Integer i : cols) {
            assertEquals("short", r.schema().get(i).type().name().toLowerCase(Locale.ROOT));
        }
    }

    private void assertDriverType(
        String colName,
        int type,
        boolean hasRadix,
        int precision,
        int bufferLength,
        Class<? extends Number> typeClass,
        List<?> row
    ) {
        assertEquals(colName, name(row));
        if (hasRadix) {
            assertEquals(typeClass, radix(row).getClass());
        } else {
            assertNull(radix(row));
        }
        assertEquals(precision, precision(row));
        assertEquals(bufferLength, bufferLength(row));
        assertNull(decimalPrecision(row));
        if (typeClass != null) {
            assertEquals(type, typeClass.cast(sqlType(row)).intValue());
            assertEquals(typeClass, nullable(row).getClass());
            assertEquals(typeClass, sqlDataType(row).getClass());
            assertEquals(typeClass, sqlDataTypeSub(row).getClass());
        }
    }
}
