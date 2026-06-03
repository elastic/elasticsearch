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
 * Writes the ordinal stream of a single sorted or sorted-set field to a segment.
 *
 * <p>{@link #writeFieldEntry} owns the entire field lifecycle: it iterates the doc values source,
 * accumulates statistics, and emits both the per-field metadata and the encoded ordinal blocks.
 * {@link #encoder()} exposes the per-block {@link Encoder} so callers can drive block encoding
 * directly when needed.
 */
public interface OrdinalFieldWriter {

    /**
     * Writes one ordinal field and returns its statistics.
     *
     * @param field               field being written
     * @param valuesSource        source of doc values for this field
     * @param maxOrd              maximum ordinal value for this field
     * @param docValueCountConsumer receives the per-doc value count for sorted-set offset
     *                              tracking, or {@code null} when offsets are not needed
     * @param sortedFieldObserver receives {@code (docId, ord)} pairs during the doc pass,
     *                            or {@code null} when no observer is attached
     * @return the field's doc value count statistics
     */
    DocValueFieldCountStats writeFieldEntry(
        FieldInfo field,
        TsdbDocValuesProducer valuesSource,
        long maxOrd,
        AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        SortedFieldObserver sortedFieldObserver
    ) throws IOException;

    /**
     * Returns the per-block encoder used to encode the field's ordinal blocks.
     */
    Encoder encoder();

    /**
     * Encodes one block of ordinal values.
     */
    @FunctionalInterface
    interface Encoder {

        /**
         * Encodes one block into {@code data} using a fixed bit width.
         *
         * @param values     ordinal values to encode
         * @param data       data output to write the encoded block to
         * @param bitsPerOrd number of bits per ordinal
         */
        void encodeOrdinals(long[] values, IndexOutput data, int bitsPerOrd) throws IOException;
    }

    /**
     * Encodes one block of ordinal values with per-document context.
     *
     * <p>This is the per-doc-aware variant of {@link Encoder} for SORTED_SET fields whose
     * block encoder picks better encodings when it knows the doc boundaries inside the block
     * (e.g. tuple-run encoding). The receiver gets {@code perDocK[0..numDocs - 1]} giving the
     * full K (number of ords) of each doc whose values appear in this block, plus
     * {@code headOffset}/{@code tailMissing} indicating how many ords of the first and last
     * docs are split into neighboring blocks.
     */
    @FunctionalInterface
    interface TupleAwareEncoder {

        /**
         * Encodes one block into {@code data}.
         *
         * @param values        ordinal values to encode
         * @param perDocK       full per-doc value counts for every doc whose ords appear in
         *                      this block, including any doc that straddles a block boundary
         * @param numDocs       number of valid entries in {@code perDocK}
         * @param headOffset    number of ords of the first doc that belong to the PREVIOUS
         *                      block (0 if the block starts at a doc boundary)
         * @param tailMissing   number of ords of the last doc that belong to the NEXT block
         *                      (0 if the block ends at a doc boundary)
         * @param data          data output to write the encoded block to
         * @param bitsPerOrd    number of bits per ordinal
         */
        void encodeOrdinals(long[] values, int[] perDocK, int numDocs, int headOffset, int tailMissing, IndexOutput data, int bitsPerOrd)
            throws IOException;
    }
}
