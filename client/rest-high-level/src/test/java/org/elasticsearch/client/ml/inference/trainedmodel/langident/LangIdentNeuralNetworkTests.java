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
package org.elasticsearch.client.ml.inference.trainedmodel.langident;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;

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
