/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Reads the ordinal stream of a single sorted or sorted-set field from a segment.
 *
 * <p>{@link #readFieldEntry} runs once per field at segment-open time and parses the field metadata
 * (value counts, block index, offsets, DISI metadata) into a
 * {@link AbstractTSDBDocValuesProducer.NumericEntry}. {@link #decoder()} returns the per-block
 * {@link Decoder} that the iteration code drives during ordinal access; the same decoder may
 * be used for many blocks of the same field.
 */
public interface OrdinalFieldReader {

    /**
     * Parses the field metadata into {@code entry}.
     *
     * @param meta              segment metadata input positioned at this field's header
     * @param entry             entry to populate with the parsed metadata
     * @param numericBlockShift block shift used to size the per-field block index
     */
    void readFieldEntry(IndexInput meta, AbstractTSDBDocValuesProducer.NumericEntry entry, int numericBlockShift) throws IOException;

    /**
     * Returns the per-block decoder used to decode the field's ordinal blocks.
     *
     * @return the block decoder
     */
    Decoder decoder();

    /**
     * Decodes one block of ordinal values.
     */
    @FunctionalInterface
    interface Decoder {

        /**
         * Decodes the next block from {@code input} into {@code values}.
         *
         * @param input      data input positioned at the start of the block
         * @param values     output array to fill with ordinal values
         * @param bitsPerOrd number of bits per ordinal in the encoded block
         */
        void decodeOrdinals(DataInput input, long[] values, int bitsPerOrd) throws IOException;
    }
}
