/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;
import org.elasticsearch.xpack.core.deprecation.LoggingDeprecationAccumulationHandler;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.TransformMessages;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class QueryConfig implements SimpleDiffable<QueryConfig>, Writeable, ToXContentObject {
    private static final Logger logger = LogManager.getLogger(QueryConfig.class);

    // we store the query in 2 formats: the raw format and the parsed format, because:
    // - the parsed format adds defaults, which were not part of the original and looks odd on XContent retrieval
    // - if parsing fails (e.g. query uses removed functionality), the source can be retrieved
    private final Map<String, Object> source;
    private final QueryBuilder query;

    public static QueryConfig matchAll() {
        return new QueryConfig(Collections.singletonMap(MatchAllQueryBuilder.NAME, Collections.emptyMap()), new MatchAllQueryBuilder());
    }

    public QueryConfig(final Map<String, Object> source, final QueryBuilder query) {
        this.source = Objects.requireNonNull(source);
        this.query = query;
    }

    public QueryConfig(final StreamInput in) throws IOException {
        this.source = in.readMap();
        this.query = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(source);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(source);
        out.writeOptionalNamedWriteable(query);
    }

    public QueryBuilder getQuery() {
        return query;
    }

    public static QueryConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        // we need 2 passes, but the parser can not be cloned, so we parse 1st into a map and then re-parse that for syntax checking

        // remember the registry, needed for the 2nd pass
        NamedXContentRegistry registry = parser.getXContentRegistry();

        Map<String, Object> source = parser.mapOrdered();
        QueryBuilder query = null;

        try {
            query = queryFromXContent(source, registry, LoggingDeprecationHandler.INSTANCE);
        } catch (Exception e) {
            if (lenient) {
                logger.warn(TransformMessages.LOG_TRANSFORM_CONFIGURATION_BAD_QUERY, e);
            } else {
                throw e;
            }
        }

        return new QueryConfig(source, query);
    }

    private static QueryBuilder queryFromXContent(
        Map<String, Object> source,
        NamedXContentRegistry namedXContentRegistry,
        DeprecationHandler deprecationHandler
    ) throws IOException {
        QueryBuilder query = null;
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(source);
        XContentParser sourceParser = XContentType.JSON.xContent()
            .createParser(namedXContentRegistry, deprecationHandler, BytesReference.bytes(xContentBuilder).streamInput());
        query = AbstractQueryBuilder.parseTopLevelQuery(sourceParser);

        return query;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, query);
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

        return Objects.equals(this.source, that.source) && Objects.equals(this.query, that.query);
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (query == null) {
            validationException = addValidationError("source.query must not be null", validationException);
        }
        return validationException;
    }

    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {
        LoggingDeprecationAccumulationHandler deprecationLogger = new LoggingDeprecationAccumulationHandler();

        try {
            queryFromXContent(source, namedXContentRegistry, deprecationLogger);
        } catch (Exception e) {
            onDeprecation.accept(
                new DeprecationIssue(
                    Level.CRITICAL,
                    "Transform [" + id + "]: " + TransformMessages.LOG_TRANSFORM_CONFIGURATION_BAD_QUERY,
                    TransformDeprecations.QUERY_BREAKING_CHANGES_URL,
                    e.getMessage(),
                    false,
                    null
                )
            );
        }

        deprecationLogger.getDeprecations().forEach(deprecationMessage -> {
            onDeprecation.accept(
                new DeprecationIssue(
                    Level.CRITICAL,
                    "Transform [" + id + "] uses deprecated query options",
                    TransformDeprecations.QUERY_BREAKING_CHANGES_URL,
                    deprecationMessage,
                    false,
                    null
                )
            );
        });

    }
}
