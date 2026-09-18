/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankServiceSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Body of an OCI Generative AI {@code rerankText} request.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/datatypes/RerankTextDetails">RerankTextDetails</a>
 */
public record OciGenAiRerankRequestEntity(
    String query,
    List<String> documents,
    OciGenAiRerankServiceSettings serviceSettings,
    @Nullable Integer topN,
    @Nullable Boolean returnDocuments
) implements ToXContentObject {

    static final String INPUT_FIELD = "input";
    static final String DOCUMENTS_FIELD = "documents";
    static final String TOP_N_FIELD = "topN";
    static final String IS_ECHO_FIELD = "isEcho";

    public OciGenAiRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(serviceSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);
        OciGenAiRequestUtils.writeCompartmentAndServingMode(builder, serviceSettings);
        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        }
        if (returnDocuments != null) {
            builder.field(IS_ECHO_FIELD, returnDocuments);
        }
        builder.endObject();
        return builder;
    }
}
