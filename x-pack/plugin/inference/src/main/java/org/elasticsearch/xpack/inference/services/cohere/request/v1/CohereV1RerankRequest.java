/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v1;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankModel;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CohereV1RerankRequest extends CohereRequest {

    private final String query;
    private final List<String> input;
    private final Boolean returnDocuments;
    private final Integer topN;
    private final CohereRerankTaskSettings taskSettings;

    public CohereV1RerankRequest(
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        CohereRerankModel model
    ) {
        super(CohereAccount.of(model), model.getInferenceEntityId(), model.getServiceSettings().modelId(), false);

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
            builder.field(CohereRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments);
        } else if (taskSettings.getDoesReturnDocuments() != null) {
            builder.field(CohereRerankTaskSettings.RETURN_DOCUMENTS, taskSettings.getDoesReturnDocuments());
        }

        // prefer the root level top_n over task settings
        if (topN != null) {
            builder.field(CohereRerankTaskSettings.TOP_N_DOCS_ONLY, topN);
        } else if (taskSettings.getTopNDocumentsOnly() != null) {
            builder.field(CohereRerankTaskSettings.TOP_N_DOCS_ONLY, taskSettings.getTopNDocumentsOnly());
        }

        if (taskSettings.getMaxChunksPerDoc() != null) {
            builder.field(CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC, taskSettings.getMaxChunksPerDoc());
        }

        builder.endObject();
        return builder;
    }
}
