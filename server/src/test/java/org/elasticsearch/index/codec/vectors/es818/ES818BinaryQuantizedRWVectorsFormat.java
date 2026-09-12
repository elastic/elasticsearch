/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class ES818BinaryQuantizedRWVectorsFormat extends ES818BinaryQuantizedVectorsFormat {

    private final boolean mergeQueryDataForGraphBuild;
    private final int hnswGraphThreshold;

    /** A flat-only writer: its merges build no graph and produce no query-side records. */
    public ES818BinaryQuantizedRWVectorsFormat() {
        this(false, 0);
    }

    /**
     * @param mergeQueryDataForGraphBuild {@code true} when an HNSW format writes through this one, so merges
     *     that will build a graph also write the query-side records its merge scorer reads
     * @param hnswGraphThreshold that HNSW format's graph threshold
     */
    public ES818BinaryQuantizedRWVectorsFormat(boolean mergeQueryDataForGraphBuild, int hnswGraphThreshold) {
        this.mergeQueryDataForGraphBuild = mergeQueryDataForGraphBuild;
        this.hnswGraphThreshold = hnswGraphThreshold;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES818BinaryQuantizedVectorsWriter(
            scorer,
            rawVectorFormat.fieldsWriter(state),
            state,
            mergeQueryDataForGraphBuild,
            hnswGraphThreshold
        );
    }
}
