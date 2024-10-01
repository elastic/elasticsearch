/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Before;

public class ES815HnswBitVectorsFormatTests extends BaseKnnBitVectorsFormatTestCase {

    @Override
    protected Codec getCodec() {
        return new Lucene912Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES815HnswBitVectorsFormat();
            }
        };
    }

    @Before
    public void init() {
        similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    }
}
