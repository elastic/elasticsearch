/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

class QueryProvider implements Writeable, ToXContentObject {

    private static final Logger logger = LogManager.getLogger(AggProvider.class);

    private Exception parsingException;
    private QueryBuilder parsedQuery;
    private Map<String, Object> query;

    static QueryProvider defaultQuery() {
        return new QueryProvider(
            Collections.singletonMap(MatchAllQueryBuilder.NAME, Collections.emptyMap()),
            QueryBuilders.matchAllQuery(),
            null);
    }

    static QueryProvider fromXContent(XContentParser parser, boolean lenient) throws IOException {
        Map<String, Object> query = parser.mapOrdered();
        QueryBuilder parsedQuery = null;
        Exception exception = null;
        try {
            parsedQuery = XContentObjectTransformer.queryBuilderTransformer(parser.getXContentRegistry()).fromMap(query);
        } catch(Exception ex) {
            if (ex.getCause() instanceof IllegalArgumentException) {
                ex = (Exception)ex.getCause();
            }
            exception = ex;
            if (lenient) {
                logger.warn(Messages.DATAFEED_CONFIG_QUERY_BAD_FORMAT, ex);
            } else {
                throw ExceptionsHelper.badRequestException(Messages.DATAFEED_CONFIG_QUERY_BAD_FORMAT, ex);
            }
        }
        return new QueryProvider(query, parsedQuery, exception);
    }

    static QueryProvider fromParsedQuery(QueryBuilder parsedQuery) throws IOException {
        return parsedQuery == null ?
            null :
            new QueryProvider(
                XContentObjectTransformer.queryBuilderTransformer(NamedXContentRegistry.EMPTY).toMap(parsedQuery),
                parsedQuery,
                null);
    }

    static QueryProvider fromStream(StreamInput in) throws IOException {
        return new QueryProvider(in.readMap(), in.readOptionalNamedWriteable(QueryBuilder.class), in.readException());
    }

    QueryProvider(Map<String, Object> query, QueryBuilder parsedQuery, Exception parsingException) {
        this.query = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(query, "[query] must not be null")));
        this.parsedQuery = parsedQuery;
        this.parsingException = parsingException;
    }

    QueryProvider(QueryProvider other) {
        this(other.query, other.parsedQuery, other.parsingException);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(query);
        out.writeOptionalNamedWriteable(parsedQuery);
        out.writeException(parsingException);
    }

    public Exception getParsingException() {
        return parsingException;
    }

    public QueryBuilder getParsedQuery() {
        return parsedQuery;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        QueryProvider that = (QueryProvider) other;

        return Objects.equals(this.query, that.query)
            && Objects.equals(this.parsedQuery, that.parsedQuery)
            && Objects.equals(this.parsingException, that.parsingException);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, parsedQuery, parsingException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(query);
        return builder;
    }
}

