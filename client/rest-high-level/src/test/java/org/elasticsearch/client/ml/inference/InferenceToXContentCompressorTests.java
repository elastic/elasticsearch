/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
