/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.xcontent.Text;

/**
 * A row-scan view of a single column across all rows of an {@link EirfBatch}.
 *
 * <p>Each getter delegates to the corresponding {@link EirfRowReader} for the given document,
 * performing a column scan within that row to locate the value. There is no performance
 * benefit over row-by-row access on the current row-major layout; this class exists so the
 * {@link SourceColumn} API is fully exercisable today and a future column-major implementation
 * can drop in without consumer changes.
 */
public final class EirfColumn implements SourceColumn {

    private final SourceBatch batch;
    private final int colIdx;

    EirfColumn(SourceBatch batch, int columnIndex) {
        if (columnIndex < 0 || columnIndex >= batch.columnCount()) {
            throw new IndexOutOfBoundsException("columnIndex " + columnIndex + " out of range [0, " + batch.columnCount() + ")");
        }
        this.batch = batch;
        this.colIdx = columnIndex;
    }

    @Override
    public int columnIndex() {
        return colIdx;
    }

    @Override
    public int docCount() {
        return batch.docCount();
    }

    @Override
    public byte getTypeByte(int docIndex) {
        return batch.row(docIndex).getTypeByte(colIdx);
    }

    @Override
    public boolean isAbsent(int docIndex) {
        return batch.row(docIndex).isAbsent(colIdx);
    }

    @Override
    public boolean isNull(int docIndex) {
        return batch.row(docIndex).isNull(colIdx);
    }

    @Override
    public boolean getBooleanValue(int docIndex) {
        return batch.row(docIndex).getBooleanValue(colIdx);
    }

    @Override
    public int getIntValue(int docIndex) {
        return batch.row(docIndex).getIntValue(colIdx);
    }

    @Override
    public float getFloatValue(int docIndex) {
        return batch.row(docIndex).getFloatValue(colIdx);
    }

    @Override
    public long getLongValue(int docIndex) {
        return batch.row(docIndex).getLongValue(colIdx);
    }

    @Override
    public double getDoubleValue(int docIndex) {
        return batch.row(docIndex).getDoubleValue(colIdx);
    }

    @Override
    public Text getStringValue(int docIndex) {
        return batch.row(docIndex).getStringValue(colIdx);
    }

    @Override
    public BytesRef getBinaryValue(int docIndex) {
        return batch.row(docIndex).getBinaryValue(colIdx);
    }

    @Override
    public EirfKeyValueReader getKeyValue(int docIndex) {
        return batch.row(docIndex).getKeyValue(colIdx);
    }

    @Override
    public EirfArrayReader getArrayValue(int docIndex) {
        return batch.row(docIndex).getArrayValue(colIdx);
    }
}
