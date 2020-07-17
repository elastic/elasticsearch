/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;

public class PutTrainedModelActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        String modelId = randomAlphaOfLength(10);
        return new Response(TrainedModelConfigTests.createTestInstance(modelId)
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .build());
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return (in) -> {
            Response response = new Response(in);
            response.getResponse().ensureParsedDefinition(xContentRegistry());
            return response;
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
}
