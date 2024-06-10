/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.Request;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;

import java.io.IOException;

import static org.hamcrest.Matchers.contains;

public class PutTrainedModelActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        String modelId = randomAlphaOfLength(10);
        return new Request(
            TrainedModelConfigTests.createTestInstance(modelId, false)
                .setParsedDefinition(TrainedModelDefinitionTests.createSmallRandomBuilder())
                .build(),
            randomBoolean(),
            randomBoolean()
        );
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return (in) -> {
            Request request = new Request(in);
            request.getTrainedModelConfig().ensureParsedDefinition(xContentRegistry());
            return request;
        };
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }

    public void testDefaultInput() throws IOException {
        var restRequest = """
            {
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), restRequest)) {
            var request = PutTrainedModelAction.Request.parseRequest(".elser_model_2", false, false, parser);
            // The request parser sets the default input
            assertThat(request.getTrainedModelConfig().getInput().getFieldNames(), contains("text_field"));
        }
    }
}
