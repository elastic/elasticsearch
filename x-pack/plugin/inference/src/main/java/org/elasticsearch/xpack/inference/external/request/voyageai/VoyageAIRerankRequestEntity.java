/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.voyageai;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record VoyageAIRerankRequestEntity(String model, String query, List<String> documents, VoyageAIRerankTaskSettings taskSettings)
    implements
        ToXContentObject {

    private static final String DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";
    private static final String MODEL_FIELD = "model";
    public static final String TRUNCATION_FIELD = "truncation";
    public static final String RETURN_DOCUMENTS_FIELD = "return_documents";

    public VoyageAIRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);
    }

    public VoyageAIRerankRequestEntity(String query, List<String> input, VoyageAIRerankTaskSettings taskSettings, String model) {
        this(model, query, input, taskSettings != null ? taskSettings : VoyageAIRerankTaskSettings.EMPTY_SETTINGS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, model);
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        if (taskSettings.getDoesReturnDocuments() != null) {
            builder.field(VoyageAIRerankTaskSettings.RETURN_DOCUMENTS, taskSettings.getDoesReturnDocuments());
        }

        if (taskSettings.getTopKDocumentsOnly() != null) {
            builder.field(VoyageAIRerankTaskSettings.TOP_K_DOCS_ONLY, taskSettings.getTopKDocumentsOnly());
        }

        if (taskSettings.getTruncation() != null) {
            builder.field(TRUNCATION_FIELD, taskSettings.getTruncation());
        }

        builder.endObject();
        return builder;
    }

}
