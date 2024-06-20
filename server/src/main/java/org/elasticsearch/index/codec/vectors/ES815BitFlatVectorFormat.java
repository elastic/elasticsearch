/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class ES815BitFlatVectorFormat extends KnnVectorsFormat {

    static final String NAME = "ES815BitFlatVectorFormat";

    private final FlatVectorsFormat format = new ES815BitFlatVectorsFormat();

    /**
     * Sole constructor
     */
    public ES815BitFlatVectorFormat() {
        super(NAME);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES813FlatVectorFormat.ES813FlatVectorWriter(format.fieldsWriter(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES813FlatVectorFormat.ES813FlatVectorReader(format.fieldsReader(state));
    }

    @Override
    public String toString() {
        return NAME;
    }
}
