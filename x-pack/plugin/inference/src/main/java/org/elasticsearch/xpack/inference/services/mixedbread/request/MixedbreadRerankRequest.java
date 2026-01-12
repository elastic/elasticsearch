/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadAccount;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModel;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MixedbreadRerankRequest extends MixedbreadRequest {
    private final String query;
    private final List<String> input;
    private final Boolean returnDocuments;
    private final Integer topN;
    private final MixedbreadRerankTaskSettings taskSettings;

    public MixedbreadRerankRequest(
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        MixedbreadRerankModel model
    ) {
        super(MixedbreadAccount.of(model), model.getInferenceEntityId(), model.getServiceSettings().modelId(), false);

        this.input = Objects.requireNonNull(input);
        this.query = Objects.requireNonNull(query);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
        taskSettings = model.getTaskSettings();
    }

    @Override
    protected List<String> pathSegments() {
        return List.of(CohereUtils.VERSION_1, CohereUtils.RERANK_PATH);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(CohereUtils.MODEL_FIELD, getModelId());
        builder.field(CohereUtils.QUERY_FIELD, query);
        builder.field(CohereUtils.DOCUMENTS_FIELD, input);

        // prefer the root level return_documents over task settings
        if (returnDocuments != null) {
            builder.field(MixedbreadRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments);
        } else if (taskSettings.getDoesReturnDocuments() != null) {
            builder.field(MixedbreadRerankTaskSettings.RETURN_DOCUMENTS, taskSettings.getDoesReturnDocuments());
        }

        // prefer the root level top_n over task settings
        if (topN != null) {
            builder.field(MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY, topN);
        } else if (taskSettings.getTopNDocumentsOnly() != null) {
            builder.field(MixedbreadRerankTaskSettings.TOP_N_DOCS_ONLY, taskSettings.getTopNDocumentsOnly());
        }

        builder.endObject();
        return builder;
    }

    public Integer getTopN() {
        return topN != null ? topN : taskSettings.getTopNDocumentsOnly();
    }
}
