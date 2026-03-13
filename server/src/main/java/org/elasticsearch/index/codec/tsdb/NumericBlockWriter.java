/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Encodes a block of numeric doc values into the data file during segment writing.
 *
 * <p>This is the write-side counterpart of {@link NumericBlockReader}. Each codec version
 * provides its own implementation. For example, ES819 delegates to
 * {@link TSDBDocValuesEncoder} (delta, offset, GCD, bit-pack).
 *
 * <p>Instances are created lazily by {@link AbstractTSDBDocValuesConsumer#numericBlockWriter}
 * on the first full block of values. This allows codec implementations to inspect the actual data
 * before committing to a compression strategy.
 *
 * @see NumericBlockReader
 */
public interface NumericBlockWriter {

    /**
     * Writes a block of numeric values to the data output.
     *
     * @param values    the values to encode; only the first {@code blockSize} entries are valid
     * @param blockSize the number of valid values in the array
     * @param data      the output to write compressed bytes to
     */
    void write(long[] values, int blockSize, IndexOutput data) throws IOException;

    /**
     * Writes a block of ordinal values using a fixed number of bits per ordinal.
     *
     * @param values     the ordinal values to encode
     * @param data       the output to write compressed bytes to
     * @param bitsPerOrd the number of bits per ordinal
     */
    void writeOrdinals(long[] values, IndexOutput data, int bitsPerOrd) throws IOException;
}
