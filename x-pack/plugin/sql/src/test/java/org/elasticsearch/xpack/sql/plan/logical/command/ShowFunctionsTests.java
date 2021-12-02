/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.Version.CURRENT;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.INTRODUCING_UNSIGNED_LONG;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.isTypeSupportedInVersion;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;

public class ShowFunctionsTests extends ESTestCase {

    public static final String JDBC_TYPE_GEOMETRY = "GEOMETRY";

    public void testShowFunctions() throws Exception {
        ShowFunctions showFunctions = new ShowFunctions(Source.EMPTY, null);
        SqlSession session = new SqlSession(SqlTestUtils.TEST_CFG, null, new SqlFunctionRegistry(), null, null, null, null, null, null);

        showFunctions.execute(session, wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertTrue(150 <= r.size());
            assertEquals(2, r.columnCount());
            assertEquals("AVG", r.column(0));
            assertEquals("AGGREGATE", r.column(1));
        }, ex -> fail(ex.getMessage())));
    }

    public void testShowColumns() {
        String prefix = "myIndex";
        List<List<?>> rows = new ArrayList<>();
        ShowColumns.fillInRows(loadMapping("mapping-multi-field-variation.json", true), prefix, SqlVersion.fromId(CURRENT.id), rows);

        List<List<?>> expect = asList(
            asList("bool", JDBCType.BOOLEAN.getName(), BOOLEAN.typeName()),
            asList("int", JDBCType.INTEGER.getName(), INTEGER.typeName()),
            asList("unsigned_long", JDBCType.NUMERIC.getName(), UNSIGNED_LONG.typeName()),
            asList("float", JDBCType.REAL.getName(), FLOAT.typeName()),
            asList("text", JDBCType.VARCHAR.getName(), TEXT.typeName()),
            asList("keyword", JDBCType.VARCHAR.getName(), KEYWORD.typeName()),
            asList("date", JDBCType.TIMESTAMP.getName(), DATETIME.typeName()),
            asList("date_nanos", JDBCType.TIMESTAMP.getName(), DATETIME.typeName()),
            asList("unsupported", JDBCType.OTHER.getName(), UNSUPPORTED.typeName()),
            asList("some", JDBCType.STRUCT.getName(), OBJECT.typeName()),
            asList("some.dotted", JDBCType.STRUCT.getName(), OBJECT.typeName()),
            asList("some.dotted.field", JDBCType.VARCHAR.getName(), KEYWORD.typeName()),
            asList("some.string", JDBCType.VARCHAR.getName(), TEXT.typeName()),
            asList("some.string.normalized", JDBCType.VARCHAR.getName(), KEYWORD.typeName()),
            asList("some.string.typical", JDBCType.VARCHAR.getName(), KEYWORD.typeName()),
            asList("some.ambiguous", JDBCType.VARCHAR.getName(), TEXT.typeName()),
            asList("some.ambiguous.one", JDBCType.VARCHAR.getName(), KEYWORD.typeName()),
            asList("some.ambiguous.two", JDBCType.VARCHAR.getName(), KEYWORD.typeName()),
            asList("some.ambiguous.normalized", JDBCType.VARCHAR.getName(), KEYWORD.typeName()),
            asList("foo_type", JDBCType.OTHER.getName(), UNSUPPORTED.typeName()),
            asList("point", JDBC_TYPE_GEOMETRY, GEO_POINT.typeName()),
            asList("shape", JDBC_TYPE_GEOMETRY, GEO_SHAPE.typeName()),
            asList("nested", JDBCType.STRUCT.getName(), NESTED.typeName()),
            asList("nested.point", JDBC_TYPE_GEOMETRY, GEO_POINT.typeName())
        );

        assertEquals(expect.size(), rows.size());
        assertEquals(expect.get(0).size(), rows.get(0).size());

        for (int i = 0; i < expect.size(); i++) {
            List<?> expectedRow = expect.get(i);
            List<?> receivedRow = rows.get(i);
            assertEquals("Name mismatch in row " + i, prefix + "." + expectedRow.get(0), receivedRow.get(0));
            assertEquals("Type mismatch in row " + i, expectedRow.get(1), receivedRow.get(1));
            assertEquals("Mapping mismatch in row " + i, expectedRow.get(2), receivedRow.get(2));
        }
    }

    public void testUnsignedLongFiltering() {
        Set<SqlVersion> versions = new HashSet<>(
            List.of(
                SqlVersion.fromId(INTRODUCING_UNSIGNED_LONG.id - SqlVersion.MINOR_MULTIPLIER),
                INTRODUCING_UNSIGNED_LONG,
                SqlVersion.fromId(INTRODUCING_UNSIGNED_LONG.id + SqlVersion.MINOR_MULTIPLIER),
                SqlVersion.fromId(Version.CURRENT.id)
            )
        );

        for (SqlVersion version : versions) {
            List<List<?>> rows = new ArrayList<>();
            ShowColumns.fillInRows(loadMapping("mapping-multi-field-variation.json", true), null, version, rows);
            List<String> typeNames = rows.stream().map(row -> (String) row.get(2)).collect(toList());
            assertEquals(isTypeSupportedInVersion(UNSIGNED_LONG, version), typeNames.contains(UNSIGNED_LONG.typeName()));
        }
    }
}
