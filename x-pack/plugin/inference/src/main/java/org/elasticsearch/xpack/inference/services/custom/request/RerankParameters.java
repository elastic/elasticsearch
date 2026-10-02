/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;

public class RerankParameters extends RequestParameters {

    public static RerankParameters of(QueryAndDocsInputs queryAndDocsInputs) {
        Objects.requireNonNull(queryAndDocsInputs);

        return new RerankParameters(queryAndDocsInputs);
    }

    private final QueryAndDocsInputs queryAndDocsInputs;

    private RerankParameters(QueryAndDocsInputs queryAndDocsInputs) {
        super(queryAndDocsInputs.getDocsAsStrings());
        this.queryAndDocsInputs = queryAndDocsInputs;
    }

    @Override
    protected Map<String, String> taskTypeParameters() {
        var additionalParameters = new HashMap<String, String>();
        additionalParameters.put(RerankRequest.QUERY_FIELD, toJson(queryAndDocsInputs.getQueryAsString(), RerankRequest.QUERY_FIELD));
        if (queryAndDocsInputs.getTopN() != null) {
            additionalParameters.put(RerankRequest.TOP_N_FIELD, toJson(queryAndDocsInputs.getTopN(), RerankRequest.TOP_N_FIELD));
        }

        if (queryAndDocsInputs.getReturnDocuments() != null) {
            additionalParameters.put(
                RerankRequest.RETURN_DOCUMENTS_FIELD,
                toJson(queryAndDocsInputs.getReturnDocuments(), RerankRequest.RETURN_DOCUMENTS_FIELD)
            );
        }
        return additionalParameters;
    }
}
