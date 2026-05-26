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
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

import java.io.IOException;

/**
 * Reads a single numeric field from a segment in two phases.
 *
 * <p>{@link #readFieldEntry} runs once per field at segment-open time and parses the field metadata
 * (value counts, offsets, codec-specific header, DISI metadata) into a
 * {@link AbstractTSDBDocValuesProducer.NumericEntry}. {@link #decoder(PipelineDescriptor)} returns the per-block
 * {@link Decoder} that the iteration code drives during value access; the same decoder may be
 * used for many blocks of the same field.
 */
public interface NumericFieldReader {

    /**
     * Parses the field metadata into {@code entry}.
     *
     * @param meta              segment metadata input positioned at this field's header
     * @param entry             entry to populate with the parsed metadata
     * @param numericBlockShift block shift used to size the per-field block index
     */
    void readFieldEntry(IndexInput meta, AbstractTSDBDocValuesProducer.NumericEntry entry, int numericBlockShift) throws IOException;

    /**
     * Returns the per-block decoder used to decode the field's value blocks.
     *
     * @return the block decoder
     */
    Decoder decoder(PipelineDescriptor pipelineDescriptor);

    /**
     * Decodes one block of numeric values.
     */
    interface Decoder {

        /**
         * Decodes the next block from {@code input} into {@code values}.
         *
         * @param input  data input positioned at the start of the block
         * @param values output array to fill
         * @param count  number of values in the block
         */
        void decodeBlock(DataInput input, long[] values, int count) throws IOException;
    }
}
