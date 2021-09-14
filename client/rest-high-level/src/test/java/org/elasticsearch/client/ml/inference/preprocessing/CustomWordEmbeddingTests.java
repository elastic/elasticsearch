/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;


public class CustomWordEmbeddingTests extends AbstractXContentTestCase<CustomWordEmbedding> {

    @Override
    protected CustomWordEmbedding doParseInstance(XContentParser parser) throws IOException {
        return CustomWordEmbedding.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
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

}
