/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.vectors;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InferenceStringGroupTests;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.AbstractQueryVectorBuilderTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class EmbeddingQueryVectorBuilderTests extends AbstractQueryVectorBuilderTestCase<EmbeddingQueryVectorBuilder> {

    @Override
    protected List<SearchPlugin> additionalPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    protected void doAssertClientRequest(ActionRequest request, EmbeddingQueryVectorBuilder builder) {
        assertThat(request, instanceOf(EmbeddingAction.Request.class));
        EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) request;

        assertEquals(builder.getInferenceId(), embeddingRequest.getInferenceEntityId());
        assertEquals(TaskType.ANY, embeddingRequest.getTaskType());
        assertThat(embeddingRequest.getEmbeddingRequest().inputs(), hasSize(1));

        int expectedInputSize = builder.getInput().inferenceStrings().size();
        assertThat(embeddingRequest.getEmbeddingRequest().inputs().getFirst().inferenceStrings(), hasSize(expectedInputSize));

        TimeValue expectedTimeout = builder.getTimeout() != null ? builder.getTimeout() : TIMEOUT_NOT_DETERMINED;
        assertEquals(expectedTimeout, embeddingRequest.getTimeout());
    }

    @Override
    protected ActionResponse createResponse(float[] array, EmbeddingQueryVectorBuilder builder) {
        return new InferenceAction.Response(new GenericDenseEmbeddingFloatResults(List.of(new EmbeddingFloatResults.Embedding(array))));
    }

    @Override
    protected Writeable.Reader<EmbeddingQueryVectorBuilder> instanceReader() {
        return EmbeddingQueryVectorBuilder::new;
    }

    @Override
    protected EmbeddingQueryVectorBuilder createTestInstance() {
        TimeValue timeout = randomBoolean() ? TimeValue.timeValueMillis(randomLongBetween(1, 60000)) : null;
        return new EmbeddingQueryVectorBuilder(randomAlphaOfLength(10), InferenceStringGroupTests.createRandom(), timeout);
    }

    @Override
    protected EmbeddingQueryVectorBuilder mutateInstance(EmbeddingQueryVectorBuilder instance) throws IOException {
        String inferenceId = instance.getInferenceId();
        InferenceStringGroup input = instance.getInput();
        TimeValue timeout = instance.getTimeout();

        switch (randomIntBetween(0, 2)) {
            case 0 -> inferenceId = randomValueOtherThan(instance.getInferenceId(), () -> randomAlphaOfLength(10));
            case 1 -> input = randomValueOtherThan(instance.getInput(), InferenceStringGroupTests::createRandom);
            case 2 -> timeout = timeout == null ? TimeValue.timeValueMillis(randomLongBetween(1, 60000)) : null;
            default -> throw new AssertionError("Unexpected value");
        }

        return new EmbeddingQueryVectorBuilder(inferenceId, input, timeout);
    }

    @Override
    protected EmbeddingQueryVectorBuilder doParseInstance(XContentParser parser) throws IOException {
        return EmbeddingQueryVectorBuilder.fromXContent(parser);
    }
}
