/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadConstants.INPUT_FIELD;
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadConstants.QUERY_FIELD;
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadConstants.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadConstants.TOP_K_FIELD;

public record MixedbreadRerankRequestEntity(
    String model,
    String query,
    List<String> input,
    @Nullable Integer topN,
    @Nullable Boolean returnDocuments,
    MixedbreadRerankTaskSettings taskSettings
) implements ToXContentObject {

    public MixedbreadRerankRequestEntity {
        Objects.requireNonNull(model);
        Objects.requireNonNull(query);
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, model);
        builder.field(QUERY_FIELD, query);
        builder.field(INPUT_FIELD, input);

        if (topN != null) {
            builder.field(TOP_K_FIELD, topN);
        } else if (taskSettings.getTopNDocumentsOnly() != null) {
            builder.field(TOP_K_FIELD, taskSettings.getTopNDocumentsOnly());
        }

        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        } else if (taskSettings.getReturnDocuments() != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, taskSettings.getReturnDocuments());
        }
        builder.endObject();
        return builder;
    }
}
