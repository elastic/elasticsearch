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
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class InferenceToXContentCompressorTests extends ESTestCase {

    public void testInflateAndDeflate() throws IOException {
        for(int i = 0; i < 10; i++) {
            TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder().build();
            String firstDeflate = InferenceToXContentCompressor.deflate(definition);
            TrainedModelDefinition inflatedDefinition = InferenceToXContentCompressor.inflate(firstDeflate,
                parser -> TrainedModelDefinition.fromXContent(parser).build(),
                xContentRegistry());

            // Did we inflate to the same object?
            assertThat(inflatedDefinition, equalTo(definition));
        }
    }

    public void testInflateTooLargeStream() throws IOException {
        TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder().build();
        String firstDeflate = InferenceToXContentCompressor.deflate(definition);
        BytesReference inflatedBytes = InferenceToXContentCompressor.inflate(firstDeflate, 10L);
        assertThat(inflatedBytes.length(), equalTo(10));
        try(XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            inflatedBytes,
            XContentType.JSON)) {
            expectThrows(IOException.class, () -> TrainedModelConfig.fromXContent(parser));
        }
    }

    public void testInflateGarbage() {
        expectThrows(IOException.class, () -> InferenceToXContentCompressor.inflate(randomAlphaOfLength(10), 100L));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

}
