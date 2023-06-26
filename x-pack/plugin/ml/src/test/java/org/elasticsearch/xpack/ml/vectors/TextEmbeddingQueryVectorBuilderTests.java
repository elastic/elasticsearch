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
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.AbstractQueryVectorBuilderTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TextEmbeddingQueryVectorBuilderTests extends AbstractQueryVectorBuilderTestCase<TextEmbeddingQueryVectorBuilder> {

    @Override
    protected List<SearchPlugin> additionalPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    @Override
    protected void doAssertClientRequest(ActionRequest request, TextEmbeddingQueryVectorBuilder builder) {
        assertThat(request, instanceOf(InferModelAction.Request.class));
        InferModelAction.Request inferRequest = (InferModelAction.Request) request;
        assertThat(inferRequest.getTextInput(), hasSize(1));
        assertEquals(builder.getModelText(), inferRequest.getTextInput().get(0));
        assertEquals(builder.getModelId(), inferRequest.getId());
    }

    public ActionResponse createResponse(float[] array, TextEmbeddingQueryVectorBuilder builder) {
        double[] embedding = new double[array.length];
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] = array[i];
        }
        return new InferModelAction.Response(
            List.of(new TextEmbeddingResults("foo", embedding, randomBoolean())),
            builder.getModelId(),
            true
        );
    }

    @Override
    protected Writeable.Reader<TextEmbeddingQueryVectorBuilder> instanceReader() {
        return TextEmbeddingQueryVectorBuilder::new;
    }

    @Override
    protected TextEmbeddingQueryVectorBuilder createTestInstance() {
        return new TextEmbeddingQueryVectorBuilder(randomAlphaOfLength(4), randomAlphaOfLength(4));
    }

    @Override
    protected TextEmbeddingQueryVectorBuilder mutateInstance(TextEmbeddingQueryVectorBuilder instance) throws IOException {
        return new TextEmbeddingQueryVectorBuilder(instance.getModelId() + "foo", instance.getModelText() + " bar");
    }

    @Override
    protected TextEmbeddingQueryVectorBuilder doParseInstance(XContentParser parser) throws IOException {
        return TextEmbeddingQueryVectorBuilder.fromXContent(parser);
    }
}
