/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.TestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.TypesTests;

import java.sql.JDBCType;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.mockito.Mockito.mock;

public class SysTypesTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    private Tuple<Command, SqlSession> sql(String sql) {
        EsIndex test = new EsIndex("test", TypesTests.loadMapping("mapping-multi-field-with-nested.json", true));
        Analyzer analyzer = new Analyzer(TestUtils.TEST_CFG, new FunctionRegistry(), IndexResolution.valid(test), null);
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql), false);

        IndexResolver resolver = mock(IndexResolver.class);
        SqlSession session = new SqlSession(null, null, null, resolver, null, null, null, null);
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

        cmd.execute(null, wrap(r -> {
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

    public void testSysTypesDefaultFiltering() throws Exception {
        Command cmd = sql("SYS TYPES 0").v1();

        cmd.execute(null, wrap(r -> {
            assertEquals(DataType.values().length, r.size());
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesPositiveFiltering() throws Exception {
        // boolean = 16
        Command cmd = sql("SYS TYPES " + JDBCType.BOOLEAN.getVendorTypeNumber()).v1();

        cmd.execute(null, wrap(r -> {
            assertEquals(1, r.size());
            assertEquals("BOOLEAN", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesNegativeFiltering() throws Exception {
        Command cmd = sql("SYS TYPES " + JDBCType.TINYINT.getVendorTypeNumber()).v1();

        cmd.execute(null, wrap(r -> {
            assertEquals(1, r.size());
            assertEquals("BYTE", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesMultipleMatches() throws Exception {
        Command cmd = sql("SYS TYPES " + JDBCType.VARCHAR.getVendorTypeNumber()).v1();

        cmd.execute(null, wrap(r -> {
            assertEquals(3, r.size());
            assertEquals("KEYWORD", r.column(0));
            assertTrue(r.advanceRow());
            assertEquals("TEXT", r.column(0));
            assertTrue(r.advanceRow());
            assertEquals("IP", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }
}