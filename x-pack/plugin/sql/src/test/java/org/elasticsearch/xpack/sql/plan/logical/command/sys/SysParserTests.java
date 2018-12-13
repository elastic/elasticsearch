/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysParserTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();
    private final Map<String, EsField> mapping = TypesTests.loadMapping("mapping-multi-field-with-nested.json", true);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Tuple<Command, SqlSession> sql(String sql) {
        EsIndex test = new EsIndex("test", mapping);
        Analyzer analyzer = new Analyzer(Configuration.DEFAULT, new FunctionRegistry(), IndexResolution.valid(test),
                                         new Verifier(new Metrics()));
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql), true);

        IndexResolver resolver = mock(IndexResolver.class);
        when(resolver.clusterName()).thenReturn("cluster");

        doAnswer(invocation -> {
            ((ActionListener) invocation.getArguments()[2]).onResponse(singletonList(test));
            return Void.TYPE;
        }).when(resolver).resolveAsSeparateMappings(any(), any(), any());

        SqlSession session = new SqlSession(Configuration.DEFAULT, null, null, resolver, null, null, null, null);
        return new Tuple<>(cmd, session);
    }

    public void testSysTypes() throws Exception {
        Command cmd = sql("SYS TYPES").v1();

        List<String> names = asList("BYTE", "LONG", "BINARY", "NULL", "INTEGER", "SHORT", "HALF_FLOAT", "SCALED_FLOAT", "FLOAT", "DOUBLE",
                "KEYWORD", "TEXT", "IP", "BOOLEAN", "DATE",
                "INTERVAL_YEAR", "INTERVAL_MONTH", "INTERVAL_DAY", "INTERVAL_HOUR", "INTERVAL_MINUTE", "INTERVAL_SECOND",
                "INTERVAL_YEAR_TO_MONTH", "INTERVAL_DAY_TO_HOUR", "INTERVAL_DAY_TO_MINUTE", "INTERVAL_DAY_TO_SECOND",
                "INTERVAL_HOUR_TO_MINUTE", "INTERVAL_HOUR_TO_SECOND", "INTERVAL_MINUTE_TO_SECOND",
                "UNSUPPORTED", "OBJECT", "NESTED");

        cmd.execute(null, ActionListener.wrap(r -> {
            assertEquals(19, r.columnCount());
            assertEquals(DataType.values().length, r.size());
            assertFalse(r.schema().types().contains(DataType.NULL));
            // test numeric as signed
            assertFalse(r.column(9, Boolean.class));
            // make sure precision is returned as boolean (not int)
            assertFalse(r.column(10, Boolean.class));
            // no auto-increment
            assertFalse(r.column(11, Boolean.class));
            
            for (int i = 0; i < r.size(); i++) {
                assertEquals(names.get(i), r.column(0));
                r.advanceRow();
            }

        }, ex -> fail(ex.getMessage())));
    }

    public void testSysColsNoArgs() throws Exception {
        runSysColumns("SYS COLUMNS");
    }

    public void testSysColumnEmptyCatalog() throws Exception {
        Tuple<Command, SqlSession> sql = sql("SYS COLUMNS CATALOG '' TABLE LIKE '%' LIKE '%'");

        sql.v1().execute(sql.v2(), ActionListener.wrap(r -> {
            assertEquals(24, r.columnCount());
            assertEquals(22, r.size());
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysColsTableOnlyCatalog() throws Exception {
        Tuple<Command, SqlSession> sql = sql("SYS COLUMNS CATALOG 'catalog'");

        sql.v1().execute(sql.v2(), ActionListener.wrap(r -> {
            assertEquals(24, r.columnCount());
            assertEquals(0, r.size());
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysColsTableOnlyPattern() throws Exception {
        runSysColumns("SYS COLUMNS TABLE LIKE 'test'");
    }

    public void testSysColsColOnlyPattern() throws Exception {
        runSysColumns("SYS COLUMNS LIKE '%'");
    }

    public void testSysColsTableAndColsPattern() throws Exception {
        runSysColumns("SYS COLUMNS TABLE LIKE 'test' LIKE '%'");
    }


    private void runSysColumns(String commandVariation) throws Exception {
        Tuple<Command, SqlSession> sql = sql(commandVariation);
        List<String> names = asList("bool",
                "int",
                "text",
                "keyword",
                "unsupported",
                "date",
                "some",
                "some.dotted",
                "some.dotted.field",
                "some.string",
                "some.string.normalized",
                "some.string.typical",
                "some.ambiguous",
                "some.ambiguous.one",
                "some.ambiguous.two",
                "some.ambiguous.normalized",
                "dep",
                "dep.dep_name",
                "dep.dep_id",
                "dep.dep_id.keyword",
                "dep.end_date",
                "dep.start_date");

        sql.v1().execute(sql.v2(), ActionListener.wrap(r -> {
            assertEquals(24, r.columnCount());
            assertEquals(22, r.size());

            for (int i = 0; i < r.size(); i++) {
                assertEquals("cluster", r.column(0));
                assertNull(r.column(1));
                assertEquals("test", r.column(2));
                assertEquals(names.get(i), r.column(3));
                r.advanceRow();
            }

        }, ex -> fail(ex.getMessage())));
    }
}