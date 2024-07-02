/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googleaistudio;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public record GoogleAiStudioEmbeddingsRequestEntity(List<String> inputs, String model, @Nullable Integer dimensions)
    implements
        ToXContentObject {

    private static final String REQUESTS_FIELD = "requests";
    private static final String MODEL_FIELD = "model";

    private static final String MODELS_PREFIX = "models";
    private static final String CONTENT_FIELD = "content";
    private static final String PARTS_FIELD = "parts";
    private static final String TEXT_FIELD = "text";

    private static final String OUTPUT_DIMENSIONALITY_FIELD = "outputDimensionality";

    public GoogleAiStudioEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(REQUESTS_FIELD);

        for (String input : inputs) {
            builder.startObject();
            builder.field(MODEL_FIELD, format("%s/%s", MODELS_PREFIX, model));

            {
                builder.startObject(CONTENT_FIELD);

                {
                    builder.startArray(PARTS_FIELD);

                    {
                        builder.startObject();
                        builder.field(TEXT_FIELD, input);
                        builder.endObject();
                    }

                    builder.endArray();
                }

                builder.endObject();
            }

            if (dimensions != null) {
                builder.field(OUTPUT_DIMENSIONALITY_FIELD, dimensions);
            }

            builder.endObject();
        }

        builder.endArray();
        builder.endObject();

        return builder;
    }
}
