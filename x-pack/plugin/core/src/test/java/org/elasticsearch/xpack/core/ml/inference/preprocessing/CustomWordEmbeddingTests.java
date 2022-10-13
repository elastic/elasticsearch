/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class CustomWordEmbeddingTests extends PreProcessingTests<CustomWordEmbedding> {

    @Override
    protected CustomWordEmbedding doParseInstance(XContentParser parser) throws IOException {
        return lenient ? CustomWordEmbedding.fromXContentLenient(parser) : CustomWordEmbedding.fromXContentStrict(parser);
    }

    @Override
    protected CustomWordEmbedding createTestInstance() {
        return createRandom();
    }

    public static CustomWordEmbedding createRandom() {
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
        byte[][] weights = new byte[weightsSize][internalWeightsSize];
        for (int i = 0; i < weightsSize; i++) {
            for (int j = 0; j < internalWeightsSize; j++) {
                weights[i][j] = randomByte();
            }
        }
        return new CustomWordEmbedding(quantiles, weights, randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<CustomWordEmbedding> instanceReader() {
        return CustomWordEmbedding::new;
    }

}
