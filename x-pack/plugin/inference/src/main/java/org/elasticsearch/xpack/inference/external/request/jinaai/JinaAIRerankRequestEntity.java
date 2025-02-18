/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.jinaai;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record JinaAIRerankRequestEntity(String model, String query, List<String> documents, JinaAIRerankTaskSettings taskSettings)
    implements
        ToXContentObject {

    private static final String DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";
    private static final String MODEL_FIELD = "model";

    public JinaAIRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);
    }

    public JinaAIRerankRequestEntity(String query, List<String> input, JinaAIRerankTaskSettings taskSettings, String model) {
        this(model, query, input, taskSettings != null ? taskSettings : JinaAIRerankTaskSettings.EMPTY_SETTINGS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, model);
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        if (taskSettings.getTopNDocumentsOnly() != null) {
            builder.field(JinaAIRerankTaskSettings.TOP_N_DOCS_ONLY, taskSettings.getTopNDocumentsOnly());
        }

        var return_documents = taskSettings.getDoesReturnDocuments();
        if (return_documents != null) {
            builder.field(JinaAIRerankTaskSettings.RETURN_DOCUMENTS, return_documents);
        }

        builder.endObject();
        return builder;
    }

}
