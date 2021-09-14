/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.langident;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;

import java.io.IOException;


public class LangIdentNeuralNetworkTests extends AbstractXContentTestCase<LangIdentNeuralNetwork> {

    @Override
    protected LangIdentNeuralNetwork doParseInstance(XContentParser parser) throws IOException {
        return LangIdentNeuralNetwork.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected LangIdentNeuralNetwork createTestInstance() {
        return createRandom();
    }

    public static LangIdentNeuralNetwork createRandom() {
        return new LangIdentNeuralNetwork(randomAlphaOfLength(10),
            LangNetLayerTests.createRandom(),
            LangNetLayerTests.createRandom());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

}
