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

    /**
     * Returns 0: the columnar format does not store per-document source sizes. Callers that need a
     * size proxy should track original source lengths separately.
     */
    @Override
    public int sizeInBytes() {
        return 0;
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
