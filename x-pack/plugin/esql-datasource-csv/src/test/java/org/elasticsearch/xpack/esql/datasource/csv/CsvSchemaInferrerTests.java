/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

public class CsvSchemaInferrerTests extends ESTestCase {

    public void testAllKeyword() {
        String[] cols = { "name", "city" };
        List<String[]> rows = List.of(new String[] { "Alice", "London" }, new String[] { "Bob", "Paris" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(2, schema.size());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
    }

    public void testIntegerDetection() {
        String[] cols = { "id", "age" };
        List<String[]> rows = List.of(new String[] { "1", "30" }, new String[] { "2", "25" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
    }

    public void testLongDetection() {
        String[] cols = { "big" };
        List<String[]> rows = List.of(new String[] { "9999999999" }, new String[] { "42" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.LONG, schema.get(0).dataType());
    }

    public void testDoubleDetection() {
        String[] cols = { "score" };
        List<String[]> rows = List.of(new String[] { "95.5" }, new String[] { "87.3" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testBooleanDetection() {
        String[] cols = { "active" };
        List<String[]> rows = List.of(new String[] { "true" }, new String[] { "false" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());
    }

    public void testBooleanCaseInsensitive() {
        String[] cols = { "flag" };
        List<String[]> rows = List.of(new String[] { "True" }, new String[] { "FALSE" }, new String[] { "true" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());
    }

    public void testDatetimeDetection() {
        String[] cols = { "ts" };
        List<String[]> rows = List.of(new String[] { "2021-01-01T00:00:00Z" }, new String[] { "2022-06-15T12:00:00Z" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testDateOnlyDetection() {
        String[] cols = { "date" };
        List<String[]> rows = List.of(new String[] { "2021-01-01" }, new String[] { "2022-06-15" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testZonelessTimestampDetection() {
        String[] cols = { "ts" };
        List<String[]> rows = List.<String[]>of(new String[] { "2021-01-01T10:30:00" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testMixedTypesWiden() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { "9999999999" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.LONG, schema.get(0).dataType());
    }

    public void testIntToDoubleWidening() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { "3.14" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testBooleanMismatchSkipsToKeyword() {
        String[] cols = { "flag" };
        List<String[]> rows = List.of(new String[] { "true" }, new String[] { "maybe" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testDatetimeMismatchSkipsToKeyword() {
        String[] cols = { "ts" };
        List<String[]> rows = List.of(new String[] { "2021-01-01T00:00:00Z" }, new String[] { "not_a_date" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testNullValuesPreserveCandidate() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { null }, new String[] { "" }, new String[] { "7" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testAllNullsDefaultToKeyword() {
        String[] cols = { "empty" };
        List<String[]> rows = List.of(new String[] { null }, new String[] { "" }, new String[] { "null" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testEmptyRowsDefaultToKeyword() {
        String[] cols = { "col" };
        List<String[]> rows = List.of();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testMixedColumns() {
        String[] cols = { "name", "age", "score", "active", "created" };
        List<String[]> rows = List.of(
            new String[] { "Alice", "30", "95.5", "true", "2021-01-01T00:00:00Z" },
            new String[] { "Bob", "25", "87.3", "false", "2022-06-15T12:00:00Z" }
        );
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(5, schema.size());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
        assertEquals(DataType.DOUBLE, schema.get(2).dataType());
        assertEquals(DataType.BOOLEAN, schema.get(3).dataType());
        assertEquals(DataType.DATETIME, schema.get(4).dataType());
    }

    public void testFewerValuesThanColumns() {
        String[] cols = { "a", "b", "c" };
        List<String[]> rows = List.<String[]>of(new String[] { "1", "hello" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(3, schema.size());
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
        assertEquals(DataType.KEYWORD, schema.get(2).dataType());
    }

    public void testNegativeNumbers() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "-42" }, new String[] { "-7" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testNegativeDouble() {
        String[] cols = { "value" };
        List<String[]> rows = List.<String[]>of(new String[] { "-3.14" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testColumnNames() {
        String[] cols = { " name ", " age " };
        List<String[]> rows = List.<String[]>of(new String[] { "Alice", "30" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        assertEquals("name", schema.get(0).name());
        assertEquals("age", schema.get(1).name());
    }

    public void testInferredAttributesAreNullable() {
        String[] cols = { "name", "age" };
        List<String[]> rows = List.of(new String[] { "Alice", "30" }, new String[] { "Bob", "25" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows, null);

        for (Attribute attr : schema) {
            assertEquals(Nullability.TRUE, attr.nullable());
        }
    }

    // widenSchema tests

    public void testWideningFromKeywordConflict() {
        String[] cols = { "id" };
        List<String[]> sampleRows = List.of(new String[] { "1" }, new String[] { "2" }, new String[] { "3" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        List<String[]> additionalRows = List.<String[]>of(new String[] { "hello" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.KEYWORD, widened.get(0).dataType());
    }

    public void testWideningPreservesTypeWhenNoConflict() {
        String[] cols = { "id" };
        List<String[]> sampleRows = List.of(new String[] { "1" }, new String[] { "2" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);

        List<String[]> additionalRows = List.of(new String[] { "3" }, new String[] { "4" });
        List<Attribute> result = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertSame(schema, result);
    }

    public void testWideningDoesNotJumpPastIntermediate() {
        String[] cols = { "value" };
        List<String[]> sampleRows = List.of(new String[] { "42" }, new String[] { "100" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        // A value that fits LONG but not INTEGER should widen to LONG, not skip straight to KEYWORD.
        List<String[]> additionalRows = List.<String[]>of(new String[] { "9999999999" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.LONG, widened.get(0).dataType());
    }

    public void testWideningBooleanJumpsToKeyword() {
        String[] cols = { "flag" };
        List<String[]> sampleRows = List.of(new String[] { "true" }, new String[] { "false" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());

        // Confirmed BOOLEAN hit with a non-boolean value skips directly to KEYWORD.
        List<String[]> additionalRows = List.<String[]>of(new String[] { "42" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.KEYWORD, widened.get(0).dataType());
    }

    public void testWideningAllKeywordSchemaReturnsIdentical() {
        // A schema where every column is already KEYWORD (e.g. all-null sample) should be returned
        // unchanged by widenSchema — no object allocation, assertSame passes.
        String[] cols = { "a", "b" };
        List<String[]> sampleRows = List.<String[]>of(new String[] { null, null });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());

        List<String[]> additionalRows = List.<String[]>of(new String[] { "hello", "world" });
        List<Attribute> result = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertSame(schema, result);
    }

    public void testWideningPartialColumns() {
        // Only the conflicting column widens; the non-conflicting one keeps its original Attribute object.
        String[] cols = { "id", "score" };
        List<String[]> sampleRows = List.of(new String[] { "1", "9.5" }, new String[] { "2", "8.0" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.DOUBLE, schema.get(1).dataType());

        // "id" becomes KEYWORD; "score" stays DOUBLE.
        List<String[]> additionalRows = List.<String[]>of(new String[] { "hello", "7.2" });
        List<Attribute> widened = CsvSchemaInferrer.widenSchema(schema, additionalRows, null);
        assertEquals(DataType.KEYWORD, widened.get(0).dataType());
        assertEquals(DataType.DOUBLE, widened.get(1).dataType());
        assertSame(schema.get(1), widened.get(1)); // non-widened column keeps original Attribute
    }

    public void testWideningEmptyAdditionalRows() {
        String[] cols = { "id" };
        List<String[]> sampleRows = List.<String[]>of(new String[] { "1" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, sampleRows, null);

        List<Attribute> result = CsvSchemaInferrer.widenSchema(schema, List.of(), null);
        assertSame(schema, result);
    }

    public void testSynthesizeColumnNames() {
        String[] names = CsvFormatReader.synthesizeColumnNames(4, "col");
        assertArrayEquals(new String[] { "col0", "col1", "col2", "col3" }, names);

        String[] custom = CsvFormatReader.synthesizeColumnNames(3, "f_");
        assertArrayEquals(new String[] { "f_0", "f_1", "f_2" }, custom);

        String[] zero = CsvFormatReader.synthesizeColumnNames(0, "col");
        assertEquals(0, zero.length);
    }
}
