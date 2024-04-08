/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a {@link QueryBuilder} and a list of alias names that filters the builder is composed of.
 */
public final class AliasFilter implements Writeable, Rewriteable<AliasFilter> {

    private final String[] aliases;
    private final QueryBuilder filter;

    public static final AliasFilter EMPTY = new AliasFilter(null, Strings.EMPTY_ARRAY);

    private AliasFilter(QueryBuilder filter, String... aliases) {
        this.aliases = aliases == null ? Strings.EMPTY_ARRAY : aliases;
        this.filter = filter;
    }

    public static AliasFilter of(QueryBuilder filter, String... aliases) {
        if (filter == null && (aliases == null || aliases.length == 0)) {
            return EMPTY;
        }
        return new AliasFilter(filter, aliases);
    }

    public static AliasFilter readFrom(StreamInput in) throws IOException {
        final String[] aliases = in.readStringArray();
        final QueryBuilder filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        return of(filter, aliases);
    }

    @Override
    public AliasFilter rewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder queryBuilder = this.filter;
        if (queryBuilder != null) {
            QueryBuilder rewrite = Rewriteable.rewrite(queryBuilder, context);
            if (rewrite != queryBuilder) {
                return new AliasFilter(rewrite, aliases);
            }
        }
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(aliases);
        out.writeOptionalNamedWriteable(filter);
    }

    /**
     * Returns the aliases patters that are used to compose the {@link QueryBuilder}
     * returned from {@link #getQueryBuilder()}
     */
    public String[] getAliases() {
        return aliases;
    }

    /**
     * Returns the alias filter {@link QueryBuilder} or <code>null</code> if there is no such filter
     */
    public QueryBuilder getQueryBuilder() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasFilter that = (AliasFilter) o;
        return Arrays.equals(aliases, that.aliases) && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(aliases), filter);
    }

    @Override
    public String toString() {
        return "AliasFilter{" + "aliases=" + Arrays.toString(aliases) + ", filter=" + filter + '}';
    }
}
