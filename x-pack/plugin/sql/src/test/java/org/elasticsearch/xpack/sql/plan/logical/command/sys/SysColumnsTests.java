/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.IndexResolver.IndexInfo;
import org.elasticsearch.xpack.ql.index.IndexResolver.IndexType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.stats.Metrics;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysColumnsTests extends ESTestCase {

    static final String CLUSTER_NAME = "cluster";

    private final SqlParser parser = new SqlParser();
    private final Map<String, EsField> mapping = loadMapping("mapping-multi-field-with-nested.json", true);
    private final IndexInfo index = new IndexInfo("test_emp", IndexType.STANDARD_INDEX);
    private final IndexInfo alias = new IndexInfo("alias", IndexType.ALIAS);


    public void testSysColumns() {
        List<List<?>> rows = new ArrayList<>();
        SysColumns.fillInRows("test", "index", loadMapping("mapping-multi-field-variation.json", true), null, rows, null,
                randomValueOtherThanMany(Mode::isDriver, () -> randomFrom(Mode.values())));
        // nested fields are ignored
        assertEquals(15, rows.size());
        assertEquals(24, rows.get(0).size());

        List<?> row = rows.get(0);
        assertEquals("bool", name(row));
        assertEquals(Types.BOOLEAN, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(1, bufferLength(row));

        row = rows.get(1);
        assertEquals("int", name(row));
        assertEquals(Types.INTEGER, sqlType(row));
        assertEquals(10, radix(row));
        assertEquals(4, bufferLength(row));

        row = rows.get(2);
        assertEquals("text", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        
        row = rows.get(3);
        assertEquals("keyword", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));

        row = rows.get(4);
        assertEquals("date", name(row));
        assertEquals(Types.TIMESTAMP, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(29, precision(row));
        assertEquals(8, bufferLength(row));

        row = rows.get(5);
        assertEquals("some.dotted.field", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));

        row = rows.get(6);
        assertEquals("some.string", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        
        row = rows.get(7);
        assertEquals("some.string.normalized", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        
        row = rows.get(8);
        assertEquals("some.string.typical", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        
        row = rows.get(9);
        assertEquals("some.ambiguous", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        
        row = rows.get(10);
        assertEquals("some.ambiguous.one", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        
        row = rows.get(11);
        assertEquals("some.ambiguous.two", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));

        row = rows.get(12);
        assertEquals("some.ambiguous.normalized", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
    }

    public void testSysColumnsInOdbcMode() {
        List<List<?>> rows = new ArrayList<>();
        SysColumns.fillInRows("test", "index", loadMapping("mapping-multi-field-variation.json", true), null, rows, null,
                Mode.ODBC);
        assertEquals(15, rows.size());
        assertEquals(24, rows.get(0).size());

        List<?> row = rows.get(0);
        assertEquals("bool", name(row));
        assertEquals((short) Types.BOOLEAN, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(1, bufferLength(row));

        row = rows.get(1);
        assertEquals("int", name(row));
        assertEquals((short) Types.INTEGER, sqlType(row));
        assertEquals(Short.class, radix(row).getClass());
        assertEquals(4, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(2);
        assertEquals("text", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(3);
        assertEquals("keyword", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(4);
        assertEquals("date", name(row));
        assertEquals((short) Types.TIMESTAMP, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(29, precision(row));
        assertEquals(8, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(5);
        assertEquals("some.dotted.field", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(6);
        assertEquals("some.string", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(7);
        assertEquals("some.string.normalized", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(8);
        assertEquals("some.string.typical", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(9);
        assertEquals("some.ambiguous", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());
        
        row = rows.get(10);
        assertEquals("some.ambiguous.one", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());
        
        row = rows.get(11);
        assertEquals("some.ambiguous.two", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(12);
        assertEquals("some.ambiguous.normalized", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());
    }
    
    public void testSysColumnsInJdbcMode() {
        List<List<?>> rows = new ArrayList<>();
        SysColumns.fillInRows("test", "index", loadMapping("mapping-multi-field-variation.json", true), null, rows, null,
                Mode.JDBC);
        assertEquals(15, rows.size());
        assertEquals(24, rows.get(0).size());

        List<?> row = rows.get(0);
        assertEquals("bool", name(row));
        assertEquals(Types.BOOLEAN, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(1, bufferLength(row));

        row = rows.get(1);
        assertEquals("int", name(row));
        assertEquals(Types.INTEGER, sqlType(row));
        assertEquals(Integer.class, radix(row).getClass());
        assertEquals(4, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(2);
        assertEquals("text", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(3);
        assertEquals("keyword", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(4);
        assertEquals("date", name(row));
        assertEquals(Types.TIMESTAMP, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(29, precision(row));
        assertEquals(8, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(5);
        assertEquals("some.dotted.field", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(6);
        assertEquals("some.string", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(7);
        assertEquals("some.string.normalized", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(8);
        assertEquals("some.string.typical", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());
        
        row = rows.get(9);
        assertEquals("some.ambiguous", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());
        
        row = rows.get(10);
        assertEquals("some.ambiguous.one", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());
        
        row = rows.get(11);
        assertEquals("some.ambiguous.two", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());

        row = rows.get(12);
        assertEquals("some.ambiguous.normalized", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Integer.class, nullable(row).getClass());
        assertEquals(Integer.class, sqlDataType(row).getClass());
        assertEquals(Integer.class, sqlDataTypeSub(row).getClass());
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

    public void testSysColumnsNoArg() throws Exception {
        executeCommand("SYS COLUMNS", emptyList(), r -> {
            assertEquals(13, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            // no index specified
            assertEquals("", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            // no index specified
            assertEquals("", r.column(2));
            assertEquals("int", r.column(3));
        }, mapping);
    }

    public void testSysColumnsWithCatalogWildcard() throws Exception {
        executeCommand("SYS COLUMNS CATALOG 'cluster' TABLE LIKE 'test' LIKE '%'", emptyList(), r -> {
            assertEquals(15, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("int", r.column(3));
        }, mapping);
    }

    public void testSysColumnsWithMissingCatalog() throws Exception {
        executeCommand("SYS COLUMNS TABLE LIKE 'test' LIKE '%'", emptyList(), r -> {
            assertEquals(15, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("int", r.column(3));
        }, mapping);
    }

    public void testSysColumnsWithNullCatalog() throws Exception {
        executeCommand("SYS COLUMNS CATALOG ? TABLE LIKE 'test' LIKE '%'", singletonList(new SqlTypedParamValue("keyword", null)), r -> {
            assertEquals(15, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("bool", r.column(3));
            r.advanceRow();
            assertEquals(CLUSTER_NAME, r.column(0));
            assertEquals("test", r.column(2));
            assertEquals("int", r.column(3));
        }, mapping);
    }

    @SuppressWarnings({ "unchecked" })
    private void executeCommand(String sql, List<SqlTypedParamValue> params, Consumer<SchemaRowSet> consumer, Map<String, EsField> mapping)
            throws Exception {
        Tuple<Command, SqlSession> tuple = sql(sql, params, mapping);

        IndexResolver resolver = tuple.v2().indexResolver();

        EsIndex test = new EsIndex("test", mapping);

        doAnswer(invocation -> {
            ((ActionListener<IndexResolution>) invocation.getArguments()[3]).onResponse(IndexResolution.valid(test));
            return Void.TYPE;
        }).when(resolver).resolveAsMergedMapping(any(), any(), anyBoolean(), any());

        tuple.v1().execute(tuple.v2(), wrap(p -> consumer.accept((SchemaRowSet) p.rowSet()), ex -> fail(ex.getMessage())));
    }

    private Tuple<Command, SqlSession> sql(String sql, List<SqlTypedParamValue> params, Map<String, EsField> mapping) {
        EsIndex test = new EsIndex("test", mapping);
        Analyzer analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new FunctionRegistry(), IndexResolution.valid(test),
                new Verifier(new Metrics()));
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql, params), true);

        IndexResolver resolver = mock(IndexResolver.class);
        when(resolver.clusterName()).thenReturn(CLUSTER_NAME);

        SqlSession session = new SqlSession(SqlTestUtils.TEST_CFG, null, null, resolver, null, null, null, null, null);
        return new Tuple<>(cmd, session);
    }
}
