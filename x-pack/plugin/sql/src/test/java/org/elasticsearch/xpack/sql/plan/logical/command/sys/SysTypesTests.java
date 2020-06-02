/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;

import java.sql.JDBCType;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.mockito.Mockito.mock;

public class SysTypesTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    private Tuple<Command, SqlSession> sql(String sql) {
        EsIndex test = new EsIndex("test", SqlTypesTests.loadMapping("mapping-multi-field-with-nested.json", true));
        Analyzer analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new FunctionRegistry(), IndexResolution.valid(test), null);
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql), false);

        IndexResolver resolver = mock(IndexResolver.class);
        SqlSession session = new SqlSession(SqlTestUtils.TEST_CFG, null, null, resolver, null, null, null, null, null);
        return new Tuple<>(cmd, session);
    }

    public void testSysTypes() {
        Command cmd = sql("SYS TYPES").v1();

        List<String> names = asList("BYTE", "LONG", "BINARY", "NULL", "INTEGER", "SHORT", "HALF_FLOAT", "FLOAT", "DOUBLE", "SCALED_FLOAT",
                "CONSTANT_KEYWORD", "IP", "KEYWORD", "TEXT", "BOOLEAN", "DATE", "TIME", "DATETIME",
                "INTERVAL_YEAR", "INTERVAL_MONTH", "INTERVAL_DAY", "INTERVAL_HOUR", "INTERVAL_MINUTE", "INTERVAL_SECOND",
                "INTERVAL_YEAR_TO_MONTH", "INTERVAL_DAY_TO_HOUR", "INTERVAL_DAY_TO_MINUTE", "INTERVAL_DAY_TO_SECOND",
                "INTERVAL_HOUR_TO_MINUTE", "INTERVAL_HOUR_TO_SECOND", "INTERVAL_MINUTE_TO_SECOND",
                "GEO_POINT", "GEO_SHAPE", "SHAPE", "UNSUPPORTED", "NESTED", "OBJECT");

        cmd.execute(session(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(19, r.columnCount());
            assertEquals(SqlDataTypes.types().size(), r.size());
            assertFalse(r.schema().types().contains(DataTypes.NULL));
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

    public void testSysTypesDefaultFiltering() {
        Command cmd = sql("SYS TYPES 0").v1();

        cmd.execute(session(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(SqlDataTypes.types().size(), r.size());
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesPositiveFiltering() {
        // boolean = 16
        Command cmd = sql("SYS TYPES " + JDBCType.BOOLEAN.getVendorTypeNumber()).v1();

        cmd.execute(session(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(1, r.size());
            assertEquals("BOOLEAN", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesNegativeFiltering() {
        Command cmd = sql("SYS TYPES " + JDBCType.TINYINT.getVendorTypeNumber()).v1();

        cmd.execute(session(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(1, r.size());
            assertEquals("BYTE", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesMultipleMatches() {
        Command cmd = sql("SYS TYPES " + JDBCType.VARCHAR.getVendorTypeNumber()).v1();

        cmd.execute(session(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(4, r.size());
            assertEquals("CONSTANT_KEYWORD", r.column(0));
            assertTrue(r.advanceRow());
            assertEquals("IP", r.column(0));
            assertTrue(r.advanceRow());
            assertEquals("KEYWORD", r.column(0));
            assertTrue(r.advanceRow());
            assertEquals("TEXT", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    private static SqlSession session() {
        return new SqlSession(SqlTestUtils.TEST_CFG, null, null, null, null, null, null, null, null);
    }
}
