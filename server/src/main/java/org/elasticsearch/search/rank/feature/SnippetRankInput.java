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

import java.io.IOException;
import java.util.Objects;

/**
 * Defines a custom rank input to rerank results based on snippets rather than full field contents.
 */
public class SnippetRankInput implements CustomRankInput {

    static final String NAME = "snippets";

    private final RerankSnippetConfig snippets;
    private final String inferenceText;
    private final int tokenSizeLimit;

    public SnippetRankInput(RerankSnippetConfig snippets, String inferenceText, int tokenSizeLimit) {
        this.snippets = snippets;
        this.inferenceText = inferenceText;
        this.tokenSizeLimit = tokenSizeLimit;
    }

    public SnippetRankInput(StreamInput in) throws IOException {
        this.snippets = in.readOptionalWriteable(RerankSnippetConfig::new);
        this.inferenceText = in.readString();
        this.tokenSizeLimit = in.readVInt();
    }

    public RerankSnippetConfig snippets() {
        return snippets;
    }

    public String inferenceText() {
        return inferenceText;
    }

    public Integer tokenSizeLimit() {
        return tokenSizeLimit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(snippets);
        out.writeString(inferenceText);
        out.writeVInt(tokenSizeLimit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnippetRankInput that = (SnippetRankInput) o;
        return tokenSizeLimit == that.tokenSizeLimit
            && Objects.equals(snippets, that.snippets)
            && Objects.equals(inferenceText, that.inferenceText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snippets, inferenceText, tokenSizeLimit);
    }

    @Override
    public String name() {
        return NAME;
    }
}
