/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ContinuousFeatureValue;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.FeatureValue;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ScriptDetector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.hamcrest.Matchers.instanceOf;


public class CLD3WordEmbeddingTests extends PreProcessingTests<CLD3WordEmbedding> {

    @Override
    protected CLD3WordEmbedding doParseInstance(XContentParser parser) throws IOException {
        return lenient ? CLD3WordEmbedding.fromXContentLenient(parser) : CLD3WordEmbedding.fromXContentStrict(parser);
    }

    @Override
    protected CLD3WordEmbedding createTestInstance() {
        return createRandom();
    }

    public static CLD3WordEmbedding createRandom() {
        int quantileSize = randomIntBetween(1, 10);
        int internalQuantSize = randomIntBetween(1, 10);
        short[][] quantiles = new short[quantileSize][internalQuantSize];
        for (int i = 0; i < quantileSize; i++) {
            for (int j = 0; j < internalQuantSize; j++) {
                quantiles[i][j] = randomShort();
            }
        }
        int weightsSize = randomIntBetween(1, 10);
        int internalWeightsSize = randomIntBetween(1, 10);
        int[][] weights = new int[weightsSize][internalWeightsSize];
        for (int i = 0; i < weightsSize; i++) {
            for (int j = 0; j < internalWeightsSize; j++) {
                weights[i][j] = randomInt((int)Character.MAX_VALUE);
            }
        }
        return new CLD3WordEmbedding(quantiles, weights, randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<CLD3WordEmbedding> instanceReader() {
        return CLD3WordEmbedding::new;
    }

}
