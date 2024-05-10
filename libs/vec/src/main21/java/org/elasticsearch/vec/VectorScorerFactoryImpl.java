/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.vec.internal.IndexInputUtils;
import org.elasticsearch.vec.internal.Int7DotProduct;
import org.elasticsearch.vec.internal.Int7Euclidean;
import org.elasticsearch.vec.internal.Int7MaximumInnerProduct;

import java.util.Optional;

class VectorScorerFactoryImpl implements VectorScorerFactory {

    static final VectorScorerFactoryImpl INSTANCE;

    private VectorScorerFactoryImpl() {}

    static {
        INSTANCE = NativeAccess.instance().getVectorSimilarityFunctions().map(ignore -> new VectorScorerFactoryImpl()).orElse(null);
    }

    @Override
    public Optional<VectorScorer> getInt7ScalarQuantizedVectorScorer(
        int dims,
        int maxOrd,
        float scoreCorrectionConstant,
        VectorSimilarityType similarityType,
        IndexInput input
    ) {
        input = IndexInputUtils.unwrapAndCheckInputOrNull(input);
        if (input == null) {
            return Optional.empty(); // the input type is not MemorySegment based
        }
        return Optional.of(switch (similarityType) {
            case COSINE, DOT_PRODUCT -> new Int7DotProduct(dims, maxOrd, scoreCorrectionConstant, input);
            case EUCLIDEAN -> new Int7Euclidean(dims, maxOrd, scoreCorrectionConstant, input);
            case MAXIMUM_INNER_PRODUCT -> new Int7MaximumInnerProduct(dims, maxOrd, scoreCorrectionConstant, input);
        });
    }
}
