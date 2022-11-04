/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResultsTests;
import org.junit.Before;

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
        return new InferTrainedModelDeploymentAction.Response(TextEmbeddingResultsTests.createRandomResults(), randomLongBetween(1, 200));
    }

    @Override
    protected InferTrainedModelDeploymentAction.Response mutateInstanceForVersion(
        InferTrainedModelDeploymentAction.Response instance,
        Version version
    ) {
        if (version.before(Version.V_8_6_0)) {
            return new InferTrainedModelDeploymentAction.Response(instance.getResults(), 0);
        }

        return instance;
    }
}
