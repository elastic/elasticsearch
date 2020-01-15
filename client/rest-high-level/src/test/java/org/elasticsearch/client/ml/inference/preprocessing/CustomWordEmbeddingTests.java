/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
