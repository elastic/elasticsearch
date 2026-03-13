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

import java.io.IOException;

/**
 * Reads and decodes a block of numeric doc values from the data file during segment reading.
 *
 * <p>This is the read-side counterpart of {@link NumericBlockWriter}. Each codec version
 * provides its own implementation. For example, ES819 delegates to
 * {@link TSDBDocValuesEncoder} (delta, offset, GCD, bit-pack).
 *
 * <p>Instances are created per field entry by {@link AbstractTSDBDocValuesProducer#createNumericBlockReader}
 * and used by the numeric doc values iterators returned from {@link AbstractTSDBDocValuesProducer#getNumeric}.
 *
 * @see NumericBlockWriter
 */
public interface NumericBlockReader {

    /**
     * Reads a block of numeric values from the data input.
     *
     * @param input  the input to read compressed bytes from
     * @param values the output array to fill with decoded values
     * @param count  the number of values to decode
     */
    void read(DataInput input, long[] values, int count) throws IOException;

    /**
     * Reads a block of ordinal values from the data input. Ordinals use a fixed number
     * of bits per value, which is determined by the cardinality of the ordinal space.
     *
     * @param input      the input to read compressed bytes from
     * @param values     the output array to fill with decoded ordinal values
     * @param bitsPerOrd the number of bits per ordinal
     */
    void readOrdinals(DataInput input, long[] values, int bitsPerOrd) throws IOException;
}
