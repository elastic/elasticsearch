/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.xcontent.Text;

/**
 * An {@link ArrayReader} over an {@link EscfArrayColumn} row: a forward cursor across the
 * child sub-column's elements in {@code [start, end)}. Element values are read directly from the
 * primitive child column. Columnar arrays hold only homogeneous primitives, so {@link #nestedArray()}
 * and {@link #nestedKeyValue()} are unreachable and throw.
 */
final class ColumnarArrayReader implements ArrayReader {

    private final EscfColumn child;
    private final int end;
    private int pos;

    ColumnarArrayReader(EscfColumn child, int start, int end) {
        this.child = child;
        this.end = end;
        this.pos = start - 1;
    }

    @Override
    public boolean next() {
        return ++pos < end;
    }

    @Override
    public byte type() {
        return child.typeByteForPresent(pos);
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean booleanValue() {
        return child.getBooleanValue(pos);
    }

    @Override
    public int intValue() {
        return child.getIntValue(pos);
    }

    @Override
    public float floatValue() {
        return child.getFloatValue(pos);
    }

    @Override
    public long longValue() {
        return child.getLongValue(pos);
    }

    @Override
    public double doubleValue() {
        return child.getDoubleValue(pos);
    }

    @Override
    public String stringValue() {
        return child.getStringValue(pos).string();
    }

    @Override
    public Text textValue() {
        return child.getStringValue(pos);
    }

    @Override
    public ArrayReader nestedArray() {
        throw new UnsupportedOperationException("ESCF columnar arrays hold only leaf elements");
    }

    @Override
    public KeyValueReader nestedKeyValue() {
        throw new UnsupportedOperationException("ESCF columnar arrays hold only leaf elements");
    }
}
