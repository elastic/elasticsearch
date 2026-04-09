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
 * Reads numeric doc values for a single field during segment reading.
 *
 * <p>Each codec version provides its own implementation via
 * {@link AbstractTSDBDocValuesProducer#createNumericFieldReader}. The two levels are:
 * <ul>
 *   <li>Per-field: {@link #readField} reads the full numeric entry metadata</li>
 *   <li>Per-block: {@link Decoder#decodeBlock} and {@link Decoder#decodeOrdinals} decode
 *       individual blocks during iteration</li>
 * </ul>
 *
 * @see NumericFieldWriter
 */
public interface NumericFieldReader {

    /**
     * Reads the full numeric field metadata: value counts, ordinal detection,
     * codec-specific header, offsets, and DISI.
     *
     * @param meta              the metadata input stream
     * @param entry             the numeric entry to populate
     * @param numericBlockShift the block shift for numeric encoding
     */
    void readField(IndexInput meta, AbstractTSDBDocValuesProducer.NumericEntry entry, int numericBlockShift) throws IOException;

    /**
     * Returns a decoder for per-block numeric value decoding.
     *
     * @return a new decoder instance
     */
    Decoder decoder();

    /**
     * Per-block decoder for numeric values and ordinals.
     */
    interface Decoder {

        /**
         * Decodes a block of numeric values.
         *
         * @param input  the input to read compressed bytes from
         * @param values the output array to fill with decoded values
         * @param count  the number of values to decode
         */
        void decodeBlock(DataInput input, long[] values, int count) throws IOException;

        /**
         * Decodes a block of ordinal values.
         *
         * @param input      the input to read compressed bytes from
         * @param values     the output array to fill with decoded ordinal values
         * @param bitsPerOrd the number of bits per ordinal
         */
        void decodeOrdinals(DataInput input, long[] values, int bitsPerOrd) throws IOException;
    }
}
