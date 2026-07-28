/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;

/**
 * A single-row view over an {@link EscfBatch}, backed by the batch's column vectors. Every getter
 * delegates to {@code batch.column(col).getXxx(docIndex)}, so iterating columns for one row is
 * efficient. All getters are pure reads.
 */
final class EscfRow implements SourceRow {

    private final EscfBatch batch;
    private final int docIndex;

    EscfRow(EscfBatch batch, int docIndex) {
        this.batch = batch;
        this.docIndex = docIndex;
    }

    @Override
    public SourceSchema schema() {
        return batch.schema();
    }

    @Override
    public boolean isEmpty() {
        int columnCount = batch.columnCount();
        for (int c = 0; c < columnCount; c++) {
            if (batch.column(c).isAbsent(docIndex) == false) {
                return false;
            }
        }
        return true;
    }

    // TODO: Remove this and switch the callsites to batch oriented processing. Summing each row size is inexact and expensive.
    @Override
    public int sizeInBytes() {
        int size = 0;
        int columnCount = batch.columnCount();
        for (int col = 0; col < columnCount; col++) {
            size += valueSizeInBytes(getTypeByte(col), col);
        }
        return size;
    }

    private int valueSizeInBytes(byte type, int col) {
        if (type == SourceValueType.STRING
            || type == SourceValueType.BINARY
            || type == SourceValueType.UNION_ARRAY
            || type == SourceValueType.KEY_VALUE) {
            return getBinaryValue(col).length;
        }
        if (type == SourceValueType.FIXED_ARRAY) {
            return batch.column(col).kind() == EscfColumnKind.ARRAY ? arraySizeInBytes(getArrayValue(col)) : getBinaryValue(col).length;
        }
        // ABSENT/NULL are 0 bytes; TRUE/FALSE are 1 bit (rounded to 0); LONG/DOUBLE are the only remaining (8-byte) kinds.
        return Math.max(SourceValueType.elemDataSize(type), 0);
    }

    private static int arraySizeInBytes(ArrayReader array) {
        int size = 0;
        while (array.next()) {
            byte type = array.type();
            size += type == SourceValueType.STRING ? array.textValue().bytes().length() : Math.max(SourceValueType.elemDataSize(type), 0);
        }
        return size;
    }

    @Override
    public byte getTypeByte(int col) {
        if (col < 0 || col >= batch.columnCount()) {
            return SourceValueType.ABSENT;
        }
        return batch.column(col).getTypeByte(docIndex);
    }

    @Override
    public boolean isAbsent(int col) {
        if (col < 0 || col >= batch.columnCount()) {
            return true;
        }
        return batch.column(col).isAbsent(docIndex);
    }

    @Override
    public boolean isNull(int col) {
        if (col < 0 || col >= batch.columnCount()) {
            return false;
        }
        return batch.column(col).isNull(docIndex);
    }

    @Override
    public boolean getBooleanValue(int col) {
        return batch.column(col).getBooleanValue(docIndex);
    }

    @Override
    public int getIntValue(int col) {
        return batch.column(col).getIntValue(docIndex);
    }

    @Override
    public float getFloatValue(int col) {
        return batch.column(col).getFloatValue(docIndex);
    }

    @Override
    public long getLongValue(int col) {
        return batch.column(col).getLongValue(docIndex);
    }

    @Override
    public double getDoubleValue(int col) {
        return batch.column(col).getDoubleValue(docIndex);
    }

    @Override
    public Text getStringValue(int col) {
        return batch.column(col).getStringValue(docIndex);
    }

    @Override
    public BytesRef getBinaryValue(int col) {
        return batch.column(col).getBinaryValue(docIndex);
    }

    @Override
    public KeyValueReader getKeyValue(int col) {
        return batch.column(col).getKeyValue(docIndex);
    }

    @Override
    public ArrayReader getArrayValue(int col) {
        return batch.column(col).getArrayValue(docIndex);
    }
}
