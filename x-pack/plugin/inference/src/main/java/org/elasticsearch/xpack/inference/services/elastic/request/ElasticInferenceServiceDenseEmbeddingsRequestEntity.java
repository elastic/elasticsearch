/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUsageContext;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModel;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InferenceStringGroup.toStringList;

public record ElasticInferenceServiceDenseEmbeddingsRequestEntity(
    List<InferenceStringGroup> inputs,
    ElasticInferenceServiceDenseEmbeddingsModel model,
    @Nullable ElasticInferenceServiceUsageContext usageContext
) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    private static final String USAGE_CONTEXT = "usage_context";
    private static final String DIMENSIONS = "dimensions";

    public ElasticInferenceServiceDenseEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        writeInputs(builder);

        builder.field(MODEL_FIELD, model.getServiceSettings().modelId());

        // optional field
        if (Objects.nonNull(usageContext) && usageContext != ElasticInferenceServiceUsageContext.UNSPECIFIED) {
            builder.field(USAGE_CONTEXT, usageContext);
        }

        // optional field
        var dimensions = model.getServiceSettings().dimensions();
        if (Objects.nonNull(dimensions)) {
            builder.field(DIMENSIONS, dimensions);
        }

        builder.endObject();

        return builder;
    }

    private void writeInputs(XContentBuilder builder) throws IOException {
        switch (model.getConfigurations().getTaskType()) {
            case EMBEDDING -> builder.field(INPUT_FIELD, inputs);
            case TEXT_EMBEDDING -> builder.field(INPUT_FIELD, toStringList(inputs));
            default -> throw new IllegalArgumentException(
                Strings.format(
                    "Invalid task type [%s]. Must be one of %s",
                    model.getConfigurations().getTaskType(),
                    EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)
                )
            );
        }
    }
}
