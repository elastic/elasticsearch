/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record GoogleAiStudioCompletionRequestEntity(List<String> input) implements ToXContentObject {

    private static final String CONTENTS_FIELD = "contents";

    private static final String PARTS_FIELD = "parts";

    private static final String TEXT_FIELD = "text";

    private static final String GENERATION_CONFIG_FIELD = "generationConfig";

    private static final String CANDIDATE_COUNT_FIELD = "candidateCount";

    private static final String ROLE_FIELD = "role";

    private static final String ROLE_USER = "user";

    public GoogleAiStudioCompletionRequestEntity {
        Objects.requireNonNull(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(CONTENTS_FIELD);

        {
            for (String content : input) {
                builder.startObject();

                {
                    builder.startArray(PARTS_FIELD);
                    builder.startObject();

                    {
                        builder.field(TEXT_FIELD, content);
                    }

                    builder.endObject();
                    builder.endArray();
                }

                builder.field(ROLE_FIELD, ROLE_USER);

                builder.endObject();
            }
        }

        builder.endArray();

        builder.startObject(GENERATION_CONFIG_FIELD);

        {
            // default is already 1, but we want to guard ourselves against API changes so setting it explicitly
            builder.field(CANDIDATE_COUNT_FIELD, 1);
        }

        builder.endObject();

        builder.endObject();

        return builder;
    }
}
