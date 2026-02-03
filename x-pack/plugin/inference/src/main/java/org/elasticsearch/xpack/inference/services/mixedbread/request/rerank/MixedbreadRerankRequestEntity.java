/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Entity representing the request body for Mixedbread rerank requests.
 *
 * @param modelId the model identifier
 * @param query the query string
 * @param input the list of input documents to be reranked
 * @param topN specifies the maximum number of top-ranked results to include in the response
 * @param returnDocuments specifies whether the original input documents are included in the response
 * @param taskSettings the task-specific settings to be applied
 */
public record MixedbreadRerankRequestEntity(
    String modelId,
    String query,
    List<String> input,
    @Nullable Integer topN,
    @Nullable Boolean returnDocuments,
    MixedbreadRerankTaskSettings taskSettings
) implements ToXContentObject {

    public MixedbreadRerankRequestEntity {
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(query);
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MixedbreadUtils.MODEL_FIELD, modelId);
        builder.field(MixedbreadUtils.QUERY_FIELD, query);
        builder.field(MixedbreadUtils.INPUT_FIELD, input);

        if (topN != null) {
            builder.field(MixedbreadUtils.TOP_K_FIELD, topN);
        } else if (taskSettings.getTopN() != null) {
            builder.field(MixedbreadUtils.TOP_K_FIELD, taskSettings.getTopN());
        }

        if (returnDocuments != null) {
            builder.field(MixedbreadUtils.RETURN_DOCUMENTS_FIELD, returnDocuments);
        } else if (taskSettings.getReturnDocuments() != null) {
            builder.field(MixedbreadUtils.RETURN_DOCUMENTS_FIELD, taskSettings.getReturnDocuments());
        }
        builder.endObject();
        return builder;
    }
}
