/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record CohereRerankRequestEntity(String model, String query, List<String> documents, CohereRerankTaskSettings taskSettings)
    implements
        ToXContentObject {

    private static final String DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";
    private static final String MODEL_FIELD = "model";

    public CohereRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(taskSettings);
    }

    public CohereRerankRequestEntity(String query, List<String> input, CohereRerankTaskSettings taskSettings, String model) {
        this(model, query, input, taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, model);
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        if (taskSettings.getDoesReturnDocuments() != null) {
            builder.field(CohereRerankTaskSettings.RETURN_DOCUMENTS, taskSettings.getDoesReturnDocuments());
        }

        if (taskSettings.getTopNDocumentsOnly() != null) {
            builder.field(CohereRerankTaskSettings.TOP_N_DOCS_ONLY, taskSettings.getTopNDocumentsOnly());
        }

        if (taskSettings.getMaxChunksPerDoc() != null) {
            builder.field(CohereRerankTaskSettings.MAX_CHUNKS_PER_DOC, taskSettings.getMaxChunksPerDoc());
        }

        builder.endObject();
        return builder;
    }

}
