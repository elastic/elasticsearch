/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;

public class RerankParameters extends RequestParameters {
    private static final String QUERY = "query";

    public static RerankParameters of(QueryAndDocsInputs queryAndDocsInputs) {
        Objects.requireNonNull(queryAndDocsInputs);

        return new RerankParameters(queryAndDocsInputs);
    }

    private final QueryAndDocsInputs queryAndDocsInputs;

    private RerankParameters(QueryAndDocsInputs queryAndDocsInputs) {
        super(queryAndDocsInputs.getChunks());
        this.queryAndDocsInputs = queryAndDocsInputs;
    }

    @Override
    protected Map<String, String> childParameters() {
        var additionalParameters = new HashMap<String, String>();
        additionalParameters.put(QUERY, queryAndDocsInputs.getQuery());
        if (queryAndDocsInputs.getTopN() != null) {
            additionalParameters.put(
                InferenceAction.Request.TOP_N.getPreferredName(),
                toJson(queryAndDocsInputs.getTopN(), InferenceAction.Request.TOP_N.getPreferredName())
            );
        }

        if (queryAndDocsInputs.getReturnDocuments() != null) {
            additionalParameters.put(
                InferenceAction.Request.RETURN_DOCUMENTS.getPreferredName(),
                toJson(queryAndDocsInputs.getReturnDocuments(), InferenceAction.Request.RETURN_DOCUMENTS.getPreferredName())
            );
        }
        return additionalParameters;
    }
}
