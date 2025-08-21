/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record AlibabaCloudSearchSparseRequestEntity(
    List<String> input,
    InputType inputType,
    AlibabaCloudSearchSparseTaskSettings taskSettings
) implements ToXContentObject {

    private static final String TEXTS_FIELD = "input";

    public static final String INPUT_TYPE_FIELD = "input_type";

    public static final String RETURN_TOKEN_FIELD = "return_token";

    public AlibabaCloudSearchSparseRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEXTS_FIELD, input);

        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(INPUT_TYPE_FIELD, AlibabaCloudSearchEmbeddingsRequestEntity.convertToString(inputType));
        } else if (InputType.isSpecified(taskSettings.getInputType())) {
            builder.field(INPUT_TYPE_FIELD, AlibabaCloudSearchEmbeddingsRequestEntity.convertToString(taskSettings.getInputType()));
        }

        if (taskSettings.isReturnToken() != null) {
            builder.field(RETURN_TOKEN_FIELD, taskSettings.isReturnToken());
        }
        builder.endObject();
        return builder;
    }
}
