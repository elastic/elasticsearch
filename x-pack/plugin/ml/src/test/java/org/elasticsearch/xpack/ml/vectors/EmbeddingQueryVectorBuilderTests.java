/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.vectors;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.AbstractQueryVectorBuilderTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.vectors.EmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.ml.MachineLearningTests;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class EmbeddingQueryVectorBuilderTests extends AbstractQueryVectorBuilderTestCase<EmbeddingQueryVectorBuilder> {

    @Override
    protected List<SearchPlugin> additionalPlugins() {
        return List.of(MachineLearningTests.createTrialLicensedMachineLearning(Settings.EMPTY));
    }

    @Override
    protected void doAssertClientRequest(ActionRequest request, EmbeddingQueryVectorBuilder builder) {
        assertThat(request, instanceOf(EmbeddingAction.Request.class));
        EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) request;
        assertEquals(builder.getInferenceId(), embeddingRequest.getInferenceEntityId());
        assertEquals(TaskType.EMBEDDING, embeddingRequest.getTaskType());
        assertThat(embeddingRequest.getEmbeddingRequest().inputs(), hasSize(1));
        assertThat(embeddingRequest.getEmbeddingRequest().inputs().getFirst().inferenceStrings(), hasSize(1));
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
        DataType type = randomFrom(DataType.values());
        DataFormat format = randomBoolean() ? randomFrom(type.getSupportedFormats()) : null;
        return new EmbeddingQueryVectorBuilder(randomAlphaOfLength(10), type, format, randomAlphaOfLength(20));
    }

    @Override
    protected EmbeddingQueryVectorBuilder mutateInstance(EmbeddingQueryVectorBuilder instance) throws IOException {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new EmbeddingQueryVectorBuilder(
                randomValueOtherThan(instance.getInferenceId(), () -> randomAlphaOfLength(10)),
                instance.getType(),
                instance.getFormat(),
                instance.getValue()
            );
            case 1 -> {
                DataType newType = randomValueOtherThan(instance.getType(), () -> randomFrom(DataType.values()));
                DataFormat newFormat = randomBoolean() ? randomFrom(newType.getSupportedFormats()) : null;
                yield new EmbeddingQueryVectorBuilder(instance.getInferenceId(), newType, newFormat, instance.getValue());
            }
            case 2 -> {
                DataFormat currentFormat = instance.getFormat();
                DataFormat newFormat = currentFormat == null ? randomFrom(instance.getType().getSupportedFormats()) : null;
                yield new EmbeddingQueryVectorBuilder(instance.getInferenceId(), instance.getType(), newFormat, instance.getValue());
            }
            case 3 -> new EmbeddingQueryVectorBuilder(
                instance.getInferenceId(),
                instance.getType(),
                instance.getFormat(),
                randomValueOtherThan(instance.getValue(), () -> randomAlphaOfLength(20))
            );
            default -> throw new AssertionError("Unexpected value");
        };
    }

    @Override
    protected EmbeddingQueryVectorBuilder doParseInstance(XContentParser parser) throws IOException {
        return EmbeddingQueryVectorBuilder.fromXContent(parser);
    }
}
