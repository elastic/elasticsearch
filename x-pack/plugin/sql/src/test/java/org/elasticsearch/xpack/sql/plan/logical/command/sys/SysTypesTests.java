/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.Version;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.INTRODUCING_ARRAY_TYPES;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.isTypeSupportedInVersion;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.types;
import static org.mockito.Mockito.mock;

public class SysTypesTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    private Tuple<Command, SqlSession> sql(String sql, Mode mode, SqlVersion version) {
        SqlConfiguration configuration = new SqlConfiguration(DateUtils.UTC, Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, null, mode, null, version, null, null, false, false);
        EsIndex test = new EsIndex("test", SqlTypesTests.loadMapping("mapping-multi-field-with-nested.json", true));
        Analyzer analyzer = new Analyzer(configuration, new FunctionRegistry(), IndexResolution.valid(test), null);
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql), false);

        IndexResolver resolver = mock(IndexResolver.class);
        SqlSession session = new SqlSession(configuration, null, null, resolver, null, null, null, null, null);
        return new Tuple<>(cmd, session);
    }
    private Tuple<Command, SqlSession> sql(String sql) {
        return sql(sql, randomFrom(Mode.values()), randomBoolean() ? null : SqlVersion.fromId(Version.CURRENT.id));
    }

    public void testSysTypes() {
        Tuple<Command, SqlSession> cmd = sql("SYS TYPES");

        List<String> names = asList("BYTE", "LONG", "BINARY", "NULL", "INTEGER", "SHORT", "HALF_FLOAT",
                "FLOAT", "DOUBLE", "SCALED_FLOAT", "IP", "KEYWORD", "TEXT", "BOOLEAN", "DATE", "TIME", "DATETIME",
                "INTERVAL_YEAR", "INTERVAL_MONTH", "INTERVAL_DAY", "INTERVAL_HOUR", "INTERVAL_MINUTE", "INTERVAL_SECOND",
                "INTERVAL_YEAR_TO_MONTH", "INTERVAL_DAY_TO_HOUR", "INTERVAL_DAY_TO_MINUTE", "INTERVAL_DAY_TO_SECOND",
                "INTERVAL_HOUR_TO_MINUTE", "INTERVAL_HOUR_TO_SECOND", "INTERVAL_MINUTE_TO_SECOND",
                "GEO_POINT", "GEO_SHAPE", "SHAPE", "UNSUPPORTED", "NESTED", "OBJECT");
        List<String> arrayNames = Stream.of(
                "BOOLEAN_ARRAY",
                "BYTE_ARRAY", "SHORT_ARRAY", "INTEGER_ARRAY", "LONG_ARRAY",
                "DOUBLE_ARRAY", "FLOAT_ARRAY", "HALF_FLOAT_ARRAY", "SCALED_FLOAT_ARRAY",
                "KEYWORD_ARRAY", "TEXT_ARRAY",
                "DATETIME_ARRAY",
                "IP_ARRAY",
                "BINARY_ARRAY",
                "GEO_SHAPE_ARRAY", "GEO_POINT_ARRAY",
                "SHAPE_ARRAY")
            .sorted().collect(Collectors.toList());
        List<String> typeNames = new ArrayList<>(names);
        typeNames.addAll(arrayNames);

        cmd.v1().execute(cmd.v2(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(19, r.columnCount());
            assertEquals(types().size(), r.size());
            assertFalse(r.schema().types().contains(DataTypes.NULL));
            // test first numeric (i.e. BYTE) as signed
            assertFalse(r.column(9, Boolean.class));
            // make sure precision is returned as boolean (not int)
            assertFalse(r.column(10, Boolean.class));
            // no auto-increment
            assertFalse(r.column(11, Boolean.class));

            for (int i = 0; i < r.size(); i++) {
                assertEquals(typeNames.get(i), r.column(0));
                r.advanceRow();
            }

        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesDefaultFiltering() {
        Tuple<Command, SqlSession> cmd = sql("SYS TYPES 0");

        cmd.v1().execute(cmd.v2(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(types().size(), r.size());
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesPositiveFiltering() {
        // boolean = 16
        Tuple<Command, SqlSession> cmd = sql("SYS TYPES " + JDBCType.BOOLEAN.getVendorTypeNumber());

        cmd.v1().execute(cmd.v2(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(1, r.size());
            assertEquals("BOOLEAN", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesNegativeFiltering() {
        Tuple<Command, SqlSession> cmd = sql("SYS TYPES " + JDBCType.TINYINT.getVendorTypeNumber());

        cmd.v1().execute(cmd.v2(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(1, r.size());
            assertEquals("BYTE", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    public void testSysTypesMultipleMatches() {
        Tuple<Command, SqlSession> cmd = sql("SYS TYPES " + JDBCType.VARCHAR.getVendorTypeNumber());

        cmd.v1().execute(cmd.v2(), wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertEquals(3, r.size());
            assertEquals("IP", r.column(0));
            assertTrue(r.advanceRow());
            assertEquals("KEYWORD", r.column(0));
            assertTrue(r.advanceRow());
            assertEquals("TEXT", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }

    public void testArrayTypesFiltering() {
        Set<SqlVersion> versions = new HashSet<>(List.of(
            SqlVersion.fromId(INTRODUCING_ARRAY_TYPES.id - SqlVersion.MINOR_MULTIPLIER),
            INTRODUCING_ARRAY_TYPES,
            SqlVersion.fromId(INTRODUCING_ARRAY_TYPES.id + SqlVersion.MINOR_MULTIPLIER),
            SqlVersion.fromId(Version.CURRENT.id)
        ));
        versions.add(null);

        for (SqlVersion version : versions) {
            for (Mode mode : Mode.values()) {
                Tuple<Command, SqlSession> cmd = sql("SYS TYPES", mode, version);
                SqlSession session = cmd.v2();

                cmd.v1().execute(session, wrap(p -> {
                    List<String> types = new ArrayList<>();

                    SchemaRowSet r = (SchemaRowSet) p.rowSet();
                    r.forEachRow(rv -> types.add((String) rv.column(0)));

                    for (DataType arrayType : types().stream().filter(DataTypes::isArray).collect(Collectors.toList())) {
                        assertEquals(isTypeSupportedInVersion(arrayType, session.configuration().version()),
                            types.contains(arrayType.toString()));
                    }
                }, ex -> fail(ex.getMessage())));
            }
        }
    }
}
