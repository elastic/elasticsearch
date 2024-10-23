/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.index.IndexCompatibility;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
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
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.supportsUnsignedLong;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.supportsVersionType;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;
import static org.elasticsearch.xpack.sql.util.SqlVersionUtils.UNSIGNED_LONG_TEST_VERSIONS;
import static org.elasticsearch.xpack.sql.util.SqlVersionUtils.VERSION_FIELD_TEST_VERSIONS;

public class ShowColumnsTests extends ESTestCase {

    public static final String JDBC_TYPE_GEOMETRY = "GEOMETRY";

    public void testShowColumns() {
        String prefix = "myIndex";
        List<List<?>> rows = new ArrayList<>();
        ShowColumns.fillInRows(loadMapping("mapping-multi-field-variation.json", true), prefix, rows);

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
            asList("nested.point", JDBC_TYPE_GEOMETRY, GEO_POINT.typeName()),
            asList("version", JDBCType.VARCHAR.getName(), VERSION.typeName())
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
        List<?> rowSupported = List.of("unsigned_long", "NUMERIC", "unsigned_long");
        List<?> rowUnsupported = List.of("unsigned_long", "OTHER", "unsupported");
        for (SqlVersion version : UNSIGNED_LONG_TEST_VERSIONS) {
            List<List<?>> rows = new ArrayList<>();
            // mapping's mutated by IndexCompatibility.compatible, needs to stay in the loop
            Map<String, EsField> mapping = loadMapping("mapping-multi-field-variation.json", true);
            ShowColumns.fillInRows(IndexCompatibility.compatible(mapping, version), null, rows);
            assertTrue((supportsUnsignedLong(version) && rows.contains(rowSupported)) || rows.contains(rowUnsupported));
        }
    }

    public void testVersionFieldFiltering() {
        List<?> rowSupported = List.of("version", "VARCHAR", "version");
        List<?> rowUnsupported = List.of("version", "OTHER", "unsupported");
        for (SqlVersion version : VERSION_FIELD_TEST_VERSIONS) {
            List<List<?>> rows = new ArrayList<>();
            // mapping's mutated by IndexCompatibility.compatible, needs to stay in the loop
            Map<String, EsField> mapping = loadMapping("mapping-multi-field-variation.json", true);
            ShowColumns.fillInRows(IndexCompatibility.compatible(mapping, version), null, rows);
            assertTrue((supportsVersionType(version) && rows.contains(rowSupported)) || rows.contains(rowUnsupported));
        }
    }
}
