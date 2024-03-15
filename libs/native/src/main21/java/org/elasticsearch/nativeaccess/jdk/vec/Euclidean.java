/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

// Scalar Quantized vectors are inherently bytes.
final class Euclidean extends AbstractScalarQuantizedVectorScorer {

    Euclidean(int dims, int maxOrd, float scoreCorrectionConstant, VectorDataInput data) {
        super(dims, maxOrd, scoreCorrectionConstant, data);
    }

    @Override
    public float score(int firstOrd, int secondOrd) {
        throw new UnsupportedOperationException("implement me");
    }

}
