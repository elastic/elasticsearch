/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.arrow.ArrowToEsql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The strict Arrow types of the satellite protocol (section 9.2) and the check of a satellite's answer against what ES asked for
 * (section 9.1). The mapping of Level 0 reads is wider; a fragment only carries columns whose Arrow type is in this set, because
 * a satellite must refuse to run a fragment over any other type.
 */
final class SatelliteSchemaCheck {

    private SatelliteSchemaCheck() {}

    /** The strict Arrow type of an ES|QL type, or {@code null} when the protocol has none. */
    static ArrowType strictArrowType(DataType type) {
        if (type == DataType.INTEGER) {
            return new ArrowType.Int(32, true);
        }
        if (type == DataType.LONG) {
            return new ArrowType.Int(64, true);
        }
        if (type == DataType.DOUBLE) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if (type == DataType.BOOLEAN) {
            return ArrowType.Bool.INSTANCE;
        }
        if (type == DataType.KEYWORD) {
            return ArrowType.Utf8.INSTANCE;
        }
        if (type == DataType.DATETIME) {
            return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
        }
        if (type == DataType.DATE_NANOS) {
            return new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");
        }
        return null;
    }

    /** Whether a table column is one the protocol can carry: a strict type, or a list of one. */
    static boolean isStrict(Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.List) {
            return field.getChildren().size() == 1 && isStrictScalar(field.getChildren().get(0).getType());
        }
        return isStrictScalar(type);
    }

    private static boolean isStrictScalar(ArrowType type) {
        for (DataType candidate : List.of(
            DataType.INTEGER,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.KEYWORD,
            DataType.DATETIME,
            DataType.DATE_NANOS
        )) {
            if (strictArrowType(candidate).equals(type)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks that {@code schema} is exactly what the fragment asked for: the same columns in the same order, the strict Arrow
     * type of each ES|QL type (the element type for a list), every field nullable, and no metadata. Throws before any block is
     * built from a vector of an unexpected class.
     */
    static void verify(Schema schema, List<Attribute> expected, String what) {
        List<Field> fields = schema.getFields();
        if (fields.size() != expected.size()) {
            throw mismatch(what, "has [" + fields.size() + "] fields, expected [" + expected.size() + "]");
        }
        if (schema.getCustomMetadata().isEmpty() == false) {
            throw mismatch(what, "carries schema metadata");
        }
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Attribute attribute = expected.get(i);
            if (field.getName().equals(attribute.name()) == false) {
                throw mismatch(what, "field [" + i + "] is [" + field.getName() + "], expected [" + attribute.name() + "]");
            }
            if (field.isNullable() == false || field.getMetadata().isEmpty() == false) {
                throw mismatch(what, "field [" + field.getName() + "] must be nullable and carry no metadata");
            }
            ArrowType expectedType = strictArrowType(attribute.dataType());
            Field element = field.getType() instanceof ArrowType.List && field.getChildren().size() == 1
                ? field.getChildren().get(0)
                : field;
            if (expectedType == null || expectedType.equals(element.getType()) == false) {
                throw mismatch(
                    what,
                    "field [" + field.getName() + "] has Arrow type [" + element.getType() + "], expected [" + expectedType + "]"
                );
            }
        }
    }

    /**
     * The check for a plain Flight server, which promises nothing beyond the schema ES resolved: every column the plan reads must be
     * in the stream with the ES|QL type the plan was built for. Extra columns are ignored; a missing or retyped one fails the read
     * before a block is built from a vector of the wrong class (the table may have changed since it was resolved).
     */
    static void verifyReadable(Schema schema, List<Attribute> expected, String what, int maxColumns) {
        if (schema.getFields().size() > maxColumns) {
            throw unreadable(what, "has [" + schema.getFields().size() + "] fields, the limit is [" + maxColumns + "]");
        }
        Map<String, DataType> actual = new HashMap<>();
        for (Attribute attribute : expected) {
            actual.put(attribute.name(), null);
        }
        // only the columns the plan reads are mapped: a column it never reads may be of a type ES cannot read at all
        for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
            if (actual.containsKey(field.getName())) {
                var mapping = ArrowToEsql.forField(field);
                actual.put(field.getName(), mapping == null ? null : mapping.dataType());
            }
        }
        for (Attribute attribute : expected) {
            DataType type = actual.get(attribute.name());
            if (type == null) {
                throw unreadable(what, "has no readable field [" + attribute.name() + "]");
            }
            if (type != attribute.dataType()) {
                throw unreadable(what, "field [" + attribute.name() + "] is [" + type + "], expected [" + attribute.dataType() + "]");
            }
        }
    }

    private static IllegalStateException unreadable(String what, String problem) {
        return new IllegalStateException("the Flight server's " + what + " does not match what the plan reads: " + problem);
    }

    private static IllegalStateException mismatch(String what, String problem) {
        return new IllegalStateException("the satellite's " + what + " does not match the fragment output: " + problem);
    }
}
