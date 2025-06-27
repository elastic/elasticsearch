/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es819;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.ESFlatVectorsScorer;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

// Minimal copy of Lucene99HnswVectorsFormat in order to provide an optimized scorer,
// which returns identical scores to that of the default flat vector scorer.
public class ES819HnswVectorsFormat extends KnnVectorsFormat {

    static final String NAME = "ES819HnswVectorsFormat";

    static final int MAXIMUM_MAX_CONN = Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN;
    static final int MAXIMUM_BEAM_WIDTH = Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH;

    static final FlatVectorsFormat flatVectorsFormat = new Lucene99FlatVectorsFormat(ESFlatVectorsScorer.create());

    private final int maxConn;
    private final int beamWidth;

    public ES819HnswVectorsFormat() {
        this(Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN, Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
    }

    public ES819HnswVectorsFormat(int maxConn, int beamWidth) {
        super(NAME);
        if (maxConn <= 0 || maxConn > MAXIMUM_MAX_CONN) {
            throw new IllegalArgumentException(
                "maxConn must be positive and less than or equal to " + MAXIMUM_MAX_CONN + "; maxConn=" + maxConn
            );
        }
        if (beamWidth <= 0 || beamWidth > MAXIMUM_BEAM_WIDTH) {
            throw new IllegalArgumentException(
                "beamWidth must be positive and less than or equal to " + MAXIMUM_BEAM_WIDTH + "; beamWidth=" + beamWidth
            );
        }
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.fieldsWriter(state), 1, null);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }

    @Override
    public String toString() {
        return "ES819HnswVectorsFormat(name=ES819HnswVectorsFormat, maxConn="
            + maxConn
            + ", beamWidth="
            + beamWidth
            + ", flatVectorFormat="
            + flatVectorsFormat
            + ")";
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }
}
