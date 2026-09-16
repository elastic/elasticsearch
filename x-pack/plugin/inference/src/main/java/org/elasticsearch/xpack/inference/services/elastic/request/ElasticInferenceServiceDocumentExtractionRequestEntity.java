/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * The request body for the Elastic Inference Service document extraction endpoint, which mirrors the Elasticsearch
 * document_extraction request one-to-one:
 * <pre>
 * {
 *   "model": "jina-reader",
 *   "input": [
 *     {
 *       "content": {"type": "pdf", "format": "base64", "value": "data:application/pdf;base64,..."}
 *     }
 *   ]
 * }</pre>
 */
public record ElasticInferenceServiceDocumentExtractionRequestEntity(List<InferenceString> documents, String modelId)
    implements
        ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String INPUT_FIELD = "input";
    private static final String CONTENT_FIELD = "content";

    public ElasticInferenceServiceDocumentExtractionRequestEntity {
        Objects.requireNonNull(documents);
        Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, modelId);

        builder.startArray(INPUT_FIELD);
        for (InferenceString document : documents) {
            builder.startObject();
            builder.field(CONTENT_FIELD, document);
            builder.endObject();
        }

        builder.endArray();

        builder.endObject();

        return builder;
    }
}
