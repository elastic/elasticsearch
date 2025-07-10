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

public class RerankSnippetConfig implements Writeable {

    public final Integer numSnippets;
    public final QueryBuilder snippetQueryBuilder;

    public static final int DEFAULT_NUM_SNIPPETS = 1;

    public RerankSnippetConfig(StreamInput in) throws IOException {
        this.numSnippets = in.readOptionalVInt();
        this.snippetQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    public RerankSnippetConfig(Integer numSnippets) {
        this(numSnippets, null);
    }

    public RerankSnippetConfig(Integer numSnippets, QueryBuilder snippetQueryBuilder) {
        this.numSnippets = numSnippets;
        this.snippetQueryBuilder = snippetQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(numSnippets);
        out.writeOptionalNamedWriteable(snippetQueryBuilder);
    }

    public Integer numSnippets() {
        return numSnippets;
    }

    public QueryBuilder snippetQueryBuilder() {
        return snippetQueryBuilder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RerankSnippetConfig that = (RerankSnippetConfig) o;
        return Objects.equals(numSnippets, that.numSnippets) && Objects.equals(snippetQueryBuilder, that.snippetQueryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSnippets, snippetQueryBuilder);
    }
}
