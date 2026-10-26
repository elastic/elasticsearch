/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.escf.EscfColumnBuilder;

/**
 * Streams a document's {@link ColumnarBinaryDocValuesField} payload straight into the {@link EscfColumnBuilder} that becomes the
 * field's doc-values column on the batch mapping path. The row path builds each payload in a
 * {@link StringBinaryPayload.Builder} and hands the finished blob to Lucene; a batch mapper doing the same would then copy that
 * blob into the column, so here the column's open slot is the payload's {@link StringBinaryPayload.Sink} and every value is
 * written once.
 *
 * <p>Per document: {@link #begin} at its first slot, {@link #appendSlot} for each slot, then {@link #end} to commit the payload or
 * {@link #abandon} to drop it (a bare null in strict-columnar mode, or a nullability violation the batch reports some other way).
 */
public final class ColumnarPayloadStreamer implements StringBinaryPayload.Sink {

    private final EscfColumnBuilder column;
    private final StringBinaryPayload.SinkBuilder payload = new StringBinaryPayload.SinkBuilder(this);

    public ColumnarPayloadStreamer(EscfColumnBuilder column) {
        this.column = column;
    }

    /** Opens the payload of the document at {@code row}, which must be past the last row the column has a value for. */
    public void begin(int row) {
        column.beginBytes(row);
        payload.begin();
    }

    /** Appends one slot, {@code null} denoting a null slot. */
    public void appendSlot(BytesRef value) {
        payload.appendSlot(value);
    }

    /** Commits the document's payload, count and all, as the column's value for its row. */
    public void end() {
        payload.end();
        column.endBytes();
    }

    /** Drops the document's payload; the column keeps no value for its row. */
    public void abandon() {
        column.abandonBytes();
    }

    @Override
    public void writeVInt(int value) {
        column.writeVInt(value);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int length) {
        column.writeBytes(bytes, offset, length);
    }

    @Override
    public void prefixVInt(int value, int reserved) {
        column.prefixVInt(value, reserved);
    }
}
