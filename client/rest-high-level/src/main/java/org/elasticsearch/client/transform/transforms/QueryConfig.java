/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Object for encapsulating the desired Query for a Transform
 */
public class QueryConfig implements ToXContentObject {

    private final QueryBuilder query;

    public static QueryConfig fromXContent(XContentParser parser) throws IOException {
        QueryBuilder query = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        return new QueryConfig(query);
    }

    public QueryConfig(QueryBuilder query) {
        this.query = query;
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
    public int hashCode() {
        return Objects.hash(query);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final QueryConfig that = (QueryConfig) other;

        return Objects.equals(this.query, that.query);
    }

}
