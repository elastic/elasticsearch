/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.RerankRequest;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InferenceString.textValue;
import static org.elasticsearch.inference.InferenceString.toStringList;

public class QueryAndDocsInputs extends InferenceInputs {

    public static QueryAndDocsInputs fromRerankRequest(RerankRequest request) {
        return new QueryAndDocsInputs(request.query(), request.inputs(), request.returnDocuments(), request.topN(), false);
    }

    private final InferenceString query;
    private final List<InferenceString> docs;
    private final Boolean returnDocuments;
    private final Integer topN;

    public QueryAndDocsInputs(
        InferenceString query,
        List<InferenceString> docs,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        boolean stream
    ) {
        super(stream);
        this.query = Objects.requireNonNull(query);
        this.docs = Objects.requireNonNull(docs);
        this.returnDocuments = returnDocuments;
        this.topN = topN;
    }

    public QueryAndDocsInputs(InferenceString query, List<InferenceString> docs) {
        this(query, docs, null, null, false);
    }

    public InferenceString getQuery() {
        return query;
    }

    public String getQueryAsString() {
        return textValue(query);
    }

    public List<InferenceString> getDocs() {
        return docs;
    }

    public List<String> getDocsAsStrings() {
        return toStringList(docs);
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    public Integer getTopN() {
        return topN;
    }

    @Override
    public boolean isSingleInput() {
        return docs.size() == 1;
    }
}
