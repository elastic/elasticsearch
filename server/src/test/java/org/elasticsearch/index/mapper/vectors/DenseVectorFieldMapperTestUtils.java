/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.inference.SimilarityMeasure;

import java.util.List;
import java.util.Random;

public class DenseVectorFieldMapperTestUtils {
    private DenseVectorFieldMapperTestUtils() {}

    public static List<SimilarityMeasure> getSupportedSimilarities(DenseVectorFieldMapper.ElementType elementType) {
        return switch (elementType) {
            case FLOAT, BYTE -> List.of(SimilarityMeasure.values());
            case BIT -> List.of(SimilarityMeasure.L2_NORM);
        };
    }

    public static int getEmbeddingLength(DenseVectorFieldMapper.ElementType elementType, int dimensions) {
        return switch (elementType) {
            case FLOAT, BYTE -> dimensions;
            case BIT -> {
                assert dimensions % Byte.SIZE == 0;
                yield dimensions / Byte.SIZE;
            }
        };
    }

    public static int randomCompatibleDimensions(DenseVectorFieldMapper.ElementType elementType, int max) {
        if (max < 1) {
            throw new IllegalArgumentException("max must be at least 1");
        }

        return switch (elementType) {
            case FLOAT, BYTE -> RandomNumbers.randomIntBetween(random(), 1, max);
            case BIT -> {
                if (max < 8) {
                    throw new IllegalArgumentException("max must be at least 8 for bit vectors");
                }

                // Generate a random dimension count that is a multiple of 8
                int maxEmbeddingLength = max / 8;
                yield RandomNumbers.randomIntBetween(random(), 1, maxEmbeddingLength) * 8;
            }
        };
    }

    private static Random random() {
        return RandomizedContext.current().getRandom();
    }
}
