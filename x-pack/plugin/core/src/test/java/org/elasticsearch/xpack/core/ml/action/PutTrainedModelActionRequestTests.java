/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.Request;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;

import static org.hamcrest.Matchers.nullValue;

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

    /**
     * Regression test for <a href="https://github.com/elastic/elasticsearch/issues/94854">#94854</a>: parsed definitions must not
     * appear as compressed on the master after transport, or {@link Request#validate()} returns a misleading error.
     */
    public void testDeferDefinitionValidationPreservesParsedDefinitionOnTransport() throws IOException {
        TrainedModelConfig config = TrainedModelConfigTests.createTestInstance("my-regression-model", false)
            .setModelSize(0)
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder(TargetType.REGRESSION))
            .setInferenceConfig(ClassificationConfigTests.randomClassificationConfig())
            .build();

        Request request = new Request(config, true, false);
        assertThat(request.validate(), nullValue());
        assertThat(config.getCompressedDefinitionIfSet(), nullValue());

        Request afterTransport = copyInstance(request, TransportVersion.current());
        assertThat(afterTransport.validate(), nullValue());
        assertThat(afterTransport.getTrainedModelConfig().getCompressedDefinitionIfSet(), nullValue());
    }
}
