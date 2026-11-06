/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Decodes one block of {@code long}s. It is the read side of {@link NumericBlockEncoder}. A reader depends only on
 * this interface, so {@link ProfiledBlockDecoder} can wrap it.
 */
interface NumericBlockDecoder {

    /** Decodes the first {@code valueCount} values of a block encoded by {@link NumericBlockEncoder#encode}. */
    void decode(DataInput in, int valueCount, long[] out) throws IOException;

    /**
     * The decoder for a reader: the {@link NumericBlockEncoder} of the pipeline, wrapped in a {@link ProfiledBlockDecoder}
     * when {@code metrics} is not null.
     *
     * @param metrics the recorder of the reader, or null when the reader is not profiled
     */
    static NumericBlockDecoder forReading(NumericPipeline pipeline, int blockSize, @Nullable MetricRecorder metrics) {
        final NumericBlockEncoder encoder = new NumericBlockEncoder(pipeline, blockSize);
        return metrics == null ? encoder : new ProfiledBlockDecoder(encoder, metrics);
    }
}
