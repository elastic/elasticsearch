/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Writes a single numeric field to a segment.
 *
 * <p>{@link #writeFieldEntry} owns the entire field lifecycle: it iterates the doc values source,
 * accumulates statistics, and emits both the per-field metadata and the encoded value blocks.
 * Per-block encoding is an implementation detail handled inside {@code writeFieldEntry}.
 */
public interface NumericFieldWriter {

    /**
     * Writes one numeric field and returns its statistics.
     *
     * @param field               field being written
     * @param valuesSource        source of doc values for this field
     * @param docValueCountConsumer receives the per-doc value count for sorted-numeric offset
     *                              tracking, or {@code null} when offsets are not needed
     * @param sortedFieldObserver receives {@code (docId, value)} pairs during the doc pass,
     *                            or {@code null} when no observer is attached
     * @return the field's doc value count statistics
     */
    DocValueFieldCountStats writeFieldEntry(
        FieldInfo field,
        TsdbDocValuesProducer valuesSource,
        AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        SortedFieldObserver sortedFieldObserver
    ) throws IOException;

    /**
     * Encodes one block of numeric values.
     */
    interface Encoder {

        /**
         * Encodes one block into {@code data}.
         *
         * @param values    values to encode; the first {@code blockSize} entries are valid
         * @param blockSize number of valid values in {@code values}
         * @param data      data output to write the encoded block to
         */
        void encodeBlock(long[] values, int blockSize, IndexOutput data) throws IOException;
    }
}
