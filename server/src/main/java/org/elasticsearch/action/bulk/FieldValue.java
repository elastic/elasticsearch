/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentString;

/**
 * Tagged union for a single field value extracted during single-pass parsing.
 * Holds a field path, a type tag, and the typed value. Mirrors what
 * {@link DocumentBatchEncoder.ColumnBuilder} accepts but stored per-document
 * rather than per-column.
 */
final class FieldValue {

    enum Type {
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BOOLEAN,
        BINARY,
        NULL
    }

    final String fieldPath;
    final Type type;

    // Union storage — only the field corresponding to `type` is meaningful
    int intVal;
    long longVal;
    boolean boolVal;
    XContentString strVal;
    BytesReference binVal;

    private FieldValue(String fieldPath, Type type) {
        this.fieldPath = fieldPath;
        this.type = type;
    }

    static FieldValue ofInt(String fieldPath, int value) {
        FieldValue fv = new FieldValue(fieldPath, Type.INT);
        fv.intVal = value;
        return fv;
    }

    static FieldValue ofLong(String fieldPath, long value) {
        FieldValue fv = new FieldValue(fieldPath, Type.LONG);
        fv.longVal = value;
        return fv;
    }

    static FieldValue ofFloat(String fieldPath, float value) {
        FieldValue fv = new FieldValue(fieldPath, Type.FLOAT);
        fv.intVal = Float.floatToRawIntBits(value);
        return fv;
    }

    static FieldValue ofDouble(String fieldPath, double value) {
        FieldValue fv = new FieldValue(fieldPath, Type.DOUBLE);
        fv.longVal = Double.doubleToRawLongBits(value);
        return fv;
    }

    static FieldValue ofString(String fieldPath, XContentString value) {
        FieldValue fv = new FieldValue(fieldPath, Type.STRING);
        fv.strVal = value;
        return fv;
    }

    static FieldValue ofBoolean(String fieldPath, boolean value) {
        FieldValue fv = new FieldValue(fieldPath, Type.BOOLEAN);
        fv.boolVal = value;
        return fv;
    }

    static FieldValue ofBinary(String fieldPath, BytesReference value) {
        FieldValue fv = new FieldValue(fieldPath, Type.BINARY);
        fv.binVal = value;
        return fv;
    }

    static FieldValue ofNull(String fieldPath) {
        return new FieldValue(fieldPath, Type.NULL);
    }

    /**
     * Apply this field value to a {@link DocumentBatchEncoder.ColumnBuilder} at the given document index.
     */
    void applyTo(DocumentBatchEncoder.ColumnBuilder col, int docIdx) {
        switch (type) {
            case INT -> col.setInt(docIdx, intVal);
            case LONG -> col.setLong(docIdx, longVal);
            case FLOAT -> col.setFloat(docIdx, Float.intBitsToFloat(intVal));
            case DOUBLE -> col.setDouble(docIdx, Double.longBitsToDouble(longVal));
            case STRING -> col.setOptimizedString(docIdx, strVal);
            case BOOLEAN -> col.setBoolean(docIdx, boolVal);
            case BINARY -> col.setBinary(docIdx, binVal);
            case NULL -> col.setNull(docIdx);
        }
    }
}
