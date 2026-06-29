/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.xcontent.Text;

/**
 * The fallback {@link SourceColumnCursor}: a thin forward cursor that delegates back to a column's
 * random-access getters by advancing an internal document index. Correct for any {@link SourceColumn}
 * implementation; column types that can iterate their own representation more efficiently override
 * {@link SourceColumn#cursor()} with a specialized cursor.
 */
final class RandomAccessSourceColumnCursor implements SourceColumnCursor {

    private final SourceColumn column;
    private final int docCount;
    private int doc = -1;

    RandomAccessSourceColumnCursor(SourceColumn column) {
        this.column = column;
        this.docCount = column.docCount();
    }

    @Override
    public boolean advance() {
        return ++doc < docCount;
    }

    @Override
    public byte type() {
        return column.getTypeByte(doc);
    }

    @Override
    public long longValue() {
        return column.getLongValue(doc);
    }

    @Override
    public double doubleValue() {
        return column.getDoubleValue(doc);
    }

    @Override
    public Text stringValue() {
        return column.getStringValue(doc);
    }
}
