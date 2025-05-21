/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Objects;

public class QueryAndDocsInputs extends InferenceInputs {

    public static QueryAndDocsInputs of(InferenceInputs inferenceInputs) {
        if (inferenceInputs instanceof QueryAndDocsInputs == false) {
            throw createUnsupportedTypeException(inferenceInputs, QueryAndDocsInputs.class);
        }

        return (QueryAndDocsInputs) inferenceInputs;
    }

    private final String query;
    private final List<String> chunks;
    private final Boolean returnDocuments;
    private final Integer topN;

    public QueryAndDocsInputs(String query, List<String> chunks) {
        this(query, chunks, null, null, false);
    }

    public QueryAndDocsInputs(
        String query,
        List<String> chunks,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        boolean stream
    ) {
        super(stream);
        this.query = Objects.requireNonNull(query);
        this.chunks = Objects.requireNonNull(chunks);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
    }

    public String getQuery() {
        return query;
    }

    public List<String> getChunks() {
        return chunks;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    public Integer getTopN() {
        return topN;
    }

    @Override
    public boolean isSingleInput() {
        return chunks.size() == 1;
    }
}
