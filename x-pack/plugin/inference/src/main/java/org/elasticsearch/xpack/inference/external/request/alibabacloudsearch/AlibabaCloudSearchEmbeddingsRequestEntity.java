/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.alibabacloudsearch;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettings.invalidInputTypeMessage;

public record AlibabaCloudSearchEmbeddingsRequestEntity(List<String> input, AlibabaCloudSearchEmbeddingsTaskSettings taskSettings)
    implements
        ToXContentObject {

    private static final String SEARCH_DOCUMENT = "document";
    private static final String SEARCH_QUERY = "query";

    private static final String TEXTS_FIELD = "input";

    static final String INPUT_TYPE_FIELD = "input_type";

    public AlibabaCloudSearchEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEXTS_FIELD, input);

        String inputType = covertToString(taskSettings.getInputType());
        if (inputType != null) {
            builder.field(INPUT_TYPE_FIELD, inputType);
        }

        builder.endObject();
        return builder;
    }

    // default for testing
    static String covertToString(InputType inputType) {
        if (inputType == null) {
            return null;
        }

        return switch (inputType) {
            case INGEST -> SEARCH_DOCUMENT;
            case SEARCH -> SEARCH_QUERY;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }
}
