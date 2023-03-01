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
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResultsTests;
import org.junit.Before;

import java.util.List;

public class InferTrainedModelDeploymentResponseTests extends AbstractBWCWireSerializationTestCase<
    InferTrainedModelDeploymentAction.Response> {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerNamedXContents() {
        namedXContentRegistry = new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedWriteableRegistry = new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected Writeable.Reader<InferTrainedModelDeploymentAction.Response> instanceReader() {
        return InferTrainedModelDeploymentAction.Response::new;
    }

    @Override
    protected InferTrainedModelDeploymentAction.Response createTestInstance() {
        return new InferTrainedModelDeploymentAction.Response(
            List.of(
                TextEmbeddingResultsTests.createRandomResults(),
                TextEmbeddingResultsTests.createRandomResults(),
                TextEmbeddingResultsTests.createRandomResults(),
                TextEmbeddingResultsTests.createRandomResults()
            )
        );
    }

    @Override
    protected InferTrainedModelDeploymentAction.Response mutateInstance(InferTrainedModelDeploymentAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected InferTrainedModelDeploymentAction.Response mutateInstanceForVersion(
        InferTrainedModelDeploymentAction.Response instance,
        TransportVersion version
    ) {
        if (version.before(TransportVersion.V_8_6_1)) {
            return new InferTrainedModelDeploymentAction.Response(instance.getResults().subList(0, 1));
        }

        return instance;
    }
}
