/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.feature;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Objects;

public class RerankSnippetInput implements Writeable {

    public final Integer numSnippets;
    private final String inferenceText;
    private final Integer tokenSizeLimit;
    public final QueryBuilder snippetQueryBuilder;

    public static final int DEFAULT_NUM_SNIPPETS = 1;

    public RerankSnippetInput(StreamInput in) throws IOException {
        this.numSnippets = in.readOptionalVInt();
        this.inferenceText = in.readString();
        this.tokenSizeLimit = in.readOptionalVInt();
        this.snippetQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    public RerankSnippetInput(Integer numSnippets) {
        this(numSnippets, null, null);
    }

    public RerankSnippetInput(Integer numSnippets, String inferenceText, Integer tokenSizeLimit) {
        this(numSnippets, inferenceText, tokenSizeLimit, null);
    }

    public RerankSnippetInput(Integer numSnippets, String inferenceText, Integer tokenSizeLimit, QueryBuilder snippetQueryBuilder) {
        this.numSnippets = numSnippets;
        this.inferenceText = inferenceText;
        this.tokenSizeLimit = tokenSizeLimit;
        this.snippetQueryBuilder = snippetQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(numSnippets);
        out.writeString(inferenceText);
        out.writeOptionalVInt(tokenSizeLimit);
        out.writeOptionalNamedWriteable(snippetQueryBuilder);
    }

    public Integer numSnippets() {
        return numSnippets;
    }

    public String inferenceText() {
        return inferenceText;
    }

    public Integer tokenSizeLimit() {
        return tokenSizeLimit;
    }

    public QueryBuilder snippetQueryBuilder() {
        return snippetQueryBuilder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RerankSnippetInput that = (RerankSnippetInput) o;
        return Objects.equals(numSnippets, that.numSnippets)
            && Objects.equals(inferenceText, that.inferenceText)
            && Objects.equals(tokenSizeLimit, that.tokenSizeLimit)
            && Objects.equals(snippetQueryBuilder, that.snippetQueryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSnippets, inferenceText, tokenSizeLimit, snippetQueryBuilder);
    }
}
