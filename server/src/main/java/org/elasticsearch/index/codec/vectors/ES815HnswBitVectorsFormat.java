/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.es910.ES910HnswVectorsWriter;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES815HnswBitVectorsFormat extends KnnVectorsFormat {

    static final String NAME = "ES815HnswBitVectorsFormat";

    static final int MAXIMUM_MAX_CONN = 512;
    static final int MAXIMUM_BEAM_WIDTH = 3200;

    private final int maxConn;
    private final int beamWidth;

    private static final FlatVectorsFormat flatVectorsFormat = new ES815BitFlatVectorsFormat();

    public ES815HnswBitVectorsFormat() {
        this(16, 100);
    }

    public ES815HnswBitVectorsFormat(int maxConn, int beamWidth) {
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
        return new ES910HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.fieldsWriter(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }

    @Override
    public String toString() {
        return "ES815HnswBitVectorsFormat(name=ES815HnswBitVectorsFormat, maxConn="
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
