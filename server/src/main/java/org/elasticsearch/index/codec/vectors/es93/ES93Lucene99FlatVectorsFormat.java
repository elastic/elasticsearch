/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractFlatVectorsFormat;

import java.io.IOException;

/** Lucene's flat vectors format, under the name and with the scorer Elasticsearch writes and reads it with. */
public class ES93Lucene99FlatVectorsFormat extends AbstractFlatVectorsFormat {

    static final String NAME = "Lucene99FlatVectorsFormat";

    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public ES93Lucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    public FlatVectorsScorer flatVectorsScorer() {
        return vectorsScorer;
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99FlatVectorsReader(state, vectorsScorer);
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99FlatVectorsWriter(state, vectorsScorer);
    }
}
