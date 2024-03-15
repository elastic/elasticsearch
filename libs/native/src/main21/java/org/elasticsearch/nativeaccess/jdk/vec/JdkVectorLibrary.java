/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import org.elasticsearch.nativeaccess.VectorScorer;
import org.elasticsearch.nativeaccess.VectorSimilarityType;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;

public class JdkVectorLibrary implements VectorLibrary {

    static {
        try {
            MethodHandles.lookup().ensureInitialized(NativeVectorDistance.class);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public VectorScorer getScalarQuantizedVectorScorer(int dims, int maxOrd, float scoreCorrectionConstant, VectorSimilarityType similarityType, Path path) throws IOException {
        VectorDataInput data = VectorDataInput.createVectorDataInput(path);
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> new DotProduct(dims, maxOrd, scoreCorrectionConstant, data);
            case EUCLIDEAN -> new Euclidean(dims, maxOrd, scoreCorrectionConstant, data);
            case MAXIMUM_INNER_PRODUCT -> new MaximumInnerProduct(dims, maxOrd, scoreCorrectionConstant, data);
        };
    }
}
