/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A response of an async search request.
 */
public class AsyncSearchResponse implements ToXContentObject {
    @Nullable
    private final String id;
    @Nullable
    private final SearchResponse searchResponse;
    @Nullable
    private final ElasticsearchException error;
    private final boolean isRunning;
    private final boolean isPartial;

    private final long startTimeMillis;
    private final long expirationTimeMillis;

    /**
     * Creates an {@link AsyncSearchResponse} with the arguments that are always present in the server response
     */
    AsyncSearchResponse(
        boolean isPartial,
        boolean isRunning,
        long startTimeMillis,
        long expirationTimeMillis,
        @Nullable String id,
        @Nullable SearchResponse searchResponse,
        @Nullable ElasticsearchException error
    ) {
        this.isPartial = isPartial;
        this.isRunning = isRunning;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.id = id;
        this.searchResponse = searchResponse;
        this.error = error;
    }

    /**
     * Returns the id of the async search request or null if the response is not stored in the cluster.
     */
    @Nullable
    public String getId() {
        return id;
    }

    /**
     * Returns the current {@link SearchResponse} or <code>null</code> if not available.
     *
     * See {@link #isPartial()} to determine whether the response contains partial or complete
     * results.
     */
    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    /**
     * Returns the failure reason or null if the query is running or has completed normally.
     */
    public ElasticsearchException getFailure() {
        return error;
    }

    /**
     * Returns <code>true</code> if the {@link SearchResponse} contains partial
     * results computed from a subset of the total shards.
     */
    public boolean isPartial() {
        return isPartial;
    }

    /**
     * Whether the search is still running in the cluster.
     *
     * A value of <code>false</code> indicates that the response is final
     * even if {@link #isPartial()} returns <code>true</code>. In such case,
     * the partial response represents the status of the search before a
     * non-recoverable failure.
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * When this response was created as a timestamp in milliseconds since epoch.
     */
    public long getStartTime() {
        return startTimeMillis;
    }

    /**
     * When this response will expired as a timestamp in milliseconds since epoch.
     */
    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field("id", id);
        }
        builder.field("is_partial", isPartial);
        builder.field("is_running", isRunning);
        builder.field("start_time_in_millis", startTimeMillis);
        builder.field("expiration_time_in_millis", expirationTimeMillis);

        if (searchResponse != null) {
            builder.field("response");
            searchResponse.toXContent(builder, params);
        }
        if (error != null) {
            builder.startObject("error");
            error.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField IS_PARTIAL_FIELD = new ParseField("is_partial");
    public static final ParseField IS_RUNNING_FIELD = new ParseField("is_running");
    public static final ParseField START_TIME_FIELD = new ParseField("start_time_in_millis");
    public static final ParseField EXPIRATION_FIELD = new ParseField("expiration_time_in_millis");
    public static final ParseField RESPONSE_FIELD = new ParseField("response");
    public static final ParseField ERROR_FIELD = new ParseField("error");

    public static final ConstructingObjectParser<AsyncSearchResponse, Void> PARSER = new ConstructingObjectParser<>(
        "submit_async_search_response",
        true,
        args -> new AsyncSearchResponse(
            (boolean) args[0],
            (boolean) args[1],
            (long) args[2],
            (long) args[3],
            (String) args[4],
            (SearchResponse) args[5],
            (ElasticsearchException) args[6]
        )
    );
    static {
        PARSER.declareBoolean(constructorArg(), IS_PARTIAL_FIELD);
        PARSER.declareBoolean(constructorArg(), IS_RUNNING_FIELD);
        PARSER.declareLong(constructorArg(), START_TIME_FIELD);
        PARSER.declareLong(constructorArg(), EXPIRATION_FIELD);
        PARSER.declareString(optionalConstructorArg(), ID_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> AsyncSearchResponse.parseSearchResponse(p), RESPONSE_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), ERROR_FIELD);
    }

    private static SearchResponse parseSearchResponse(XContentParser p) throws IOException {
        // we should be before the opening START_OBJECT of the response
        ensureExpectedToken(Token.START_OBJECT, p.currentToken(), p);
        p.nextToken();
        return SearchResponse.innerFromXContent(p);
    }

    public static AsyncSearchResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
