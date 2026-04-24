/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

public class CsvSchemaInferrerTests extends ESTestCase {

    public void testAllKeyword() {
        String[] cols = { "name", "city" };
        List<String[]> rows = List.of(new String[] { "Alice", "London" }, new String[] { "Bob", "Paris" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(2, schema.size());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
    }

    public void testIntegerDetection() {
        String[] cols = { "id", "age" };
        List<String[]> rows = List.of(new String[] { "1", "30" }, new String[] { "2", "25" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
    }

    public void testLongDetection() {
        String[] cols = { "big" };
        List<String[]> rows = List.of(new String[] { "9999999999" }, new String[] { "42" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.LONG, schema.get(0).dataType());
    }

    public void testDoubleDetection() {
        String[] cols = { "score" };
        List<String[]> rows = List.of(new String[] { "95.5" }, new String[] { "87.3" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testBooleanDetection() {
        String[] cols = { "active" };
        List<String[]> rows = List.of(new String[] { "true" }, new String[] { "false" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());
    }

    public void testBooleanCaseInsensitive() {
        String[] cols = { "flag" };
        List<String[]> rows = List.of(new String[] { "True" }, new String[] { "FALSE" }, new String[] { "true" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());
    }

    public void testDatetimeDetection() {
        String[] cols = { "ts" };
        List<String[]> rows = List.of(new String[] { "2021-01-01T00:00:00Z" }, new String[] { "2022-06-15T12:00:00Z" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testDateOnlyDetection() {
        String[] cols = { "date" };
        List<String[]> rows = List.of(new String[] { "2021-01-01" }, new String[] { "2022-06-15" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testZonelessTimestampDetection() {
        String[] cols = { "ts" };
        List<String[]> rows = List.<String[]>of(new String[] { "2021-01-01T10:30:00" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.DATETIME, schema.get(0).dataType());
    }

    public void testMixedTypesWiden() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { "9999999999" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.LONG, schema.get(0).dataType());
    }

    public void testIntToDoubleWidening() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { "3.14" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testBooleanMismatchSkipsToKeyword() {
        String[] cols = { "flag" };
        List<String[]> rows = List.of(new String[] { "true" }, new String[] { "maybe" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testDatetimeMismatchSkipsToKeyword() {
        String[] cols = { "ts" };
        List<String[]> rows = List.of(new String[] { "2021-01-01T00:00:00Z" }, new String[] { "not_a_date" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testNullValuesPreserveCandidate() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "42" }, new String[] { null }, new String[] { "" }, new String[] { "7" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testAllNullsDefaultToKeyword() {
        String[] cols = { "empty" };
        List<String[]> rows = List.of(new String[] { null }, new String[] { "" }, new String[] { "null" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testEmptyRowsDefaultToKeyword() {
        String[] cols = { "col" };
        List<String[]> rows = List.of();
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testMixedColumns() {
        String[] cols = { "name", "age", "score", "active", "created" };
        List<String[]> rows = List.of(
            new String[] { "Alice", "30", "95.5", "true", "2021-01-01T00:00:00Z" },
            new String[] { "Bob", "25", "87.3", "false", "2022-06-15T12:00:00Z" }
        );
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

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
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(3, schema.size());
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
        assertEquals(DataType.KEYWORD, schema.get(2).dataType());
    }

    public void testNegativeNumbers() {
        String[] cols = { "value" };
        List<String[]> rows = List.of(new String[] { "-42" }, new String[] { "-7" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testNegativeDouble() {
        String[] cols = { "value" };
        List<String[]> rows = List.<String[]>of(new String[] { "-3.14" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testColumnNames() {
        String[] cols = { " name ", " age " };
        List<String[]> rows = List.<String[]>of(new String[] { "Alice", "30" });
        List<Attribute> schema = CsvSchemaInferrer.inferSchema(cols, rows);

        assertEquals("name", schema.get(0).name());
        assertEquals("age", schema.get(1).name());
    }
}
