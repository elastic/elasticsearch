/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.index.BaseIndexFileFormatTestCase;
import org.junit.Before;

abstract class BaseKnnBitVectorsFormatTestCase extends BaseIndexFileFormatTestCase {

    @Override
    protected void addRandomFields(Document doc) {
        doc.add(new KnnByteVectorField("v2", randomVector(30), similarityFunction));
    }

    protected VectorSimilarityFunction similarityFunction;

    protected VectorSimilarityFunction randomSimilarity() {
        return VectorSimilarityFunction.values()[random().nextInt(VectorSimilarityFunction.values().length)];
    }

    byte[] randomVector(int dims) {
        byte[] vector = new byte[dims];
        random().nextBytes(vector);
        return vector;
    }

    @Before
    public void init() {
        similarityFunction = randomSimilarity();
    }
}
