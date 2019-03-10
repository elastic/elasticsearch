/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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

public class QueryProvider implements Writeable, ToXContentObject {

    private static final Logger logger = LogManager.getLogger(QueryProvider.class);

    private Exception parsingException;
    private QueryBuilder parsedQuery;
    private Map<String, Object> query;

    static QueryProvider defaultQuery() {
        try {
            QueryBuilder parsedQuery = QueryBuilders.matchAllQuery();
            Map<String, Object> query = XContentObjectTransformer.queryBuilderTransformer(NamedXContentRegistry.EMPTY).toMap(parsedQuery);
            return new QueryProvider(query, parsedQuery, null);
        } catch (IOException ex) {
            logger.error("Unexpected error creating default query", ex);
            throw new ElasticsearchException("Unexpected error creating default query", ex);
        }
    }

    static QueryProvider fromXContent(XContentParser parser, boolean lenient) throws IOException {
        Map<String, Object> query = parser.mapOrdered();
        QueryBuilder parsedQuery = null;
        Exception exception = null;
        try {
            parsedQuery = XContentObjectTransformer.queryBuilderTransformer(parser.getXContentRegistry()).fromMap(query);
            if (lenient == false) {
                query = XContentObjectTransformer.queryBuilderTransformer(parser.getXContentRegistry()).toMap(parsedQuery);
            }
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
        if (in.getVersion().onOrAfter(Version.V_6_7_0)) { // Has our bug fix for query/agg providers
            return new QueryProvider(in.readMap(), in.readOptionalNamedWriteable(QueryBuilder.class), in.readException());
        } else if (in.getVersion().onOrAfter(Version.V_6_6_0)) { // Has the bug, but supports lazy objects
            return new QueryProvider(in.readMap(), null, null);
        } else { // only supports eagerly parsed objects
            return QueryProvider.fromParsedQuery(in.readNamedWriteable(QueryBuilder.class));
        }
    }

    public QueryProvider(Map<String, Object> query, QueryBuilder parsedQuery, Exception parsingException) {
        this.query = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(query, "[query] must not be null")));
        this.parsedQuery = parsedQuery;
        this.parsingException = parsingException;
    }

    QueryProvider(QueryProvider other) {
        this(other.query, other.parsedQuery, other.parsingException);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_6_7_0)) { // Has our bug fix for query/agg providers
            out.writeMap(query);
            out.writeOptionalNamedWriteable(parsedQuery);
            out.writeException(parsingException);
        } else if (out.getVersion().onOrAfter(Version.V_6_6_0)) { // Has the bug, but supports lazy objects
            // We allow the lazy parsing nodes that have the bug throw any parsing errors themselves as
            // they already have the ability to fully parse the passed Maps
            out.writeMap(query);
        } else { // only supports eagerly parsed objects
            if (parsingException != null) { // Do we have a parsing error? Throw it
                if (parsingException instanceof IOException) {
                    throw (IOException) parsingException;
                } else {
                    throw new ElasticsearchException(parsingException);
                }
            } else if (parsedQuery == null) { // Do we have a query defined but not parsed?
                // This is an admittedly rare case but we should fail early instead of writing null when there
                // actually is a query defined
                throw new ElasticsearchException("Unsupported operation: parsed query is null");
            }
            out.writeNamedWriteable(parsedQuery);
        }
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

