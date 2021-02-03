/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Object for encapsulating the desired Query for a DataFrameAnalysis
 */
public class QueryConfig implements ToXContentObject {

    public static QueryConfig fromXContent(XContentParser parser) throws IOException {
        QueryBuilder query = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        return new QueryConfig(query);
    }

    private final QueryBuilder query;

    public QueryConfig(QueryBuilder query) {
        this.query = requireNonNull(query);
    }

    public QueryConfig(QueryConfig queryConfig) {
        this(requireNonNull(queryConfig).query);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        query.toXContent(builder, params);
        return builder;
    }

    public QueryBuilder getQuery() {
        return query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryConfig other = (QueryConfig) o;
        return Objects.equals(query, other.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
