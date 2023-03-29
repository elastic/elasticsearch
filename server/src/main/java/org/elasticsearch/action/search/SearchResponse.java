/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response of a search request.
 */
public class SearchResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField SCROLL_ID = new ParseField("_scroll_id");
    private static final ParseField POINT_IN_TIME_ID = new ParseField("pit_id");
    private static final ParseField TOOK = new ParseField("took");
    private static final ParseField TIMED_OUT = new ParseField("timed_out");
    private static final ParseField TERMINATED_EARLY = new ParseField("terminated_early");
    private static final ParseField NUM_REDUCE_PHASES = new ParseField("num_reduce_phases");

    private final SearchResponseSections internalResponse;
    private final String scrollId;
    private final String pointInTimeId;
    private final int totalShards;
    private final int successfulShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;
    private final Clusters clusters;
    private final long tookInMillis;

    public SearchResponse(StreamInput in) throws IOException {
        super(in);
        internalResponse = new InternalSearchResponse(in);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }
        clusters = new Clusters(in);
        scrollId = in.readOptionalString();
        tookInMillis = in.readVLong();
        skippedShards = in.readVInt();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            pointInTimeId = in.readOptionalString();
        } else {
            pointInTimeId = null;
        }
    }

    public SearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters
    ) {
        this(internalResponse, scrollId, totalShards, successfulShards, skippedShards, tookInMillis, shardFailures, clusters, null);
    }

    public SearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        String pointInTimeId
    ) {
        this.internalResponse = internalResponse;
        this.scrollId = scrollId;
        this.pointInTimeId = pointInTimeId;
        this.clusters = clusters;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.skippedShards = skippedShards;
        this.tookInMillis = tookInMillis;
        this.shardFailures = shardFailures;
        assert skippedShards <= totalShards : "skipped: " + skippedShards + " total: " + totalShards;
        assert scrollId == null || pointInTimeId == null
            : "SearchResponse can't have both scrollId [" + scrollId + "] and searchContextId [" + pointInTimeId + "]";
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }

    public SearchResponseSections getInternalResponse() {
        return internalResponse;
    }

    /**
     * The search hits.
     */
    public SearchHits getHits() {
        return internalResponse.hits();
    }

    /**
     * Aggregations in this response. "empty" aggregations could be
     * either {@code null} or {@link InternalAggregations#EMPTY}.
     */
    public @Nullable Aggregations getAggregations() {
        return internalResponse.aggregations();
    }

    /**
     * Will {@link #getAggregations()} return non-empty aggregation results?
     */
    public boolean hasAggregations() {
        return getAggregations() != null && getAggregations() != InternalAggregations.EMPTY;
    }

    public Suggest getSuggest() {
        return internalResponse.suggest();
    }

    /**
     * Has the search operation timed out.
     */
    public boolean isTimedOut() {
        return internalResponse.timedOut();
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    public Boolean isTerminatedEarly() {
        return internalResponse.terminatedEarly();
    }

    /**
     * Returns the number of reduce phases applied to obtain this search response
     */
    public int getNumReducePhases() {
        return internalResponse.getNumReducePhases();
    }

    /**
     * How long the search took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The number of shards skipped due to pre-filtering
     */
    public int getSkippedShards() {
        return skippedShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }

    /**
     * If scrolling was enabled ({@link SearchRequest#scroll(org.elasticsearch.search.Scroll)}, the
     * scroll id that can be used to continue scrolling.
     */
    public String getScrollId() {
        return scrollId;
    }

    /**
     * Returns the encoded string of the search context that the search request is used to executed
     */
    public String pointInTimeId() {
        return pointInTimeId;
    }

    /**
     * If profiling was enabled, this returns an object containing the profile results from
     * each shard.  If profiling was not enabled, this will return null
     *
     * @return The profile results or an empty map
     */
    @Nullable
    public Map<String, SearchProfileShardResult> getProfileResults() {
        return internalResponse.profile();
    }

    /**
     * Returns info about what clusters the search was executed against. Available only in responses obtained
     * from a Cross Cluster Search request, otherwise <code>null</code>
     * @see Clusters
     */
    public Clusters getClusters() {
        return clusters;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (scrollId != null) {
            builder.field(SCROLL_ID.getPreferredName(), scrollId);
        }
        if (pointInTimeId != null) {
            builder.field(POINT_IN_TIME_ID.getPreferredName(), pointInTimeId);
        }
        builder.field(TOOK.getPreferredName(), tookInMillis);
        builder.field(TIMED_OUT.getPreferredName(), isTimedOut());
        if (isTerminatedEarly() != null) {
            builder.field(TERMINATED_EARLY.getPreferredName(), isTerminatedEarly());
        }
        if (getNumReducePhases() != 1) {
            builder.field(NUM_REDUCE_PHASES.getPreferredName(), getNumReducePhases());
        }
        RestActions.buildBroadcastShardsHeader(
            builder,
            params,
            getTotalShards(),
            getSuccessfulShards(),
            getSkippedShards(),
            getFailedShards(),
            getShardFailures()
        );
        clusters.toXContent(builder, params);
        internalResponse.toXContent(builder, params);
        return builder;
    }

    public static SearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    public static SearchResponse innerFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName = parser.currentName();
        SearchHits hits = null;
        Aggregations aggs = null;
        Suggest suggest = null;
        SearchProfileResults profile = null;
        boolean timedOut = false;
        Boolean terminatedEarly = null;
        int numReducePhases = 1;
        long tookInMillis = -1;
        int successfulShards = -1;
        int totalShards = -1;
        int skippedShards = 0; // 0 for BWC
        String scrollId = null;
        String searchContextId = null;
        List<ShardSearchFailure> failures = new ArrayList<>();
        Clusters clusters = Clusters.EMPTY;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SCROLL_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    scrollId = parser.text();
                } else if (POINT_IN_TIME_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    searchContextId = parser.text();
                } else if (TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                    tookInMillis = parser.longValue();
                } else if (TIMED_OUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    timedOut = parser.booleanValue();
                } else if (TERMINATED_EARLY.match(currentFieldName, parser.getDeprecationHandler())) {
                    terminatedEarly = parser.booleanValue();
                } else if (NUM_REDUCE_PHASES.match(currentFieldName, parser.getDeprecationHandler())) {
                    numReducePhases = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == Token.START_OBJECT) {
                if (SearchHits.Fields.HITS.equals(currentFieldName)) {
                    hits = SearchHits.fromXContent(parser);
                } else if (Aggregations.AGGREGATIONS_FIELD.equals(currentFieldName)) {
                    aggs = Aggregations.fromXContent(parser);
                } else if (Suggest.NAME.equals(currentFieldName)) {
                    suggest = Suggest.fromXContent(parser);
                } else if (SearchProfileResults.PROFILE_FIELD.equals(currentFieldName)) {
                    profile = SearchProfileResults.fromXContent(parser);
                } else if (RestActions._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (RestActions.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.intValue(); // we don't need it but need to consume it
                            } else if (RestActions.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successfulShards = parser.intValue();
                            } else if (RestActions.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                totalShards = parser.intValue();
                            } else if (RestActions.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skippedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else if (token == Token.START_ARRAY) {
                            if (RestActions.FAILURES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                    failures.add(ShardSearchFailure.fromXContent(parser));
                                }
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else if (Clusters._CLUSTERS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    int successful = -1;
                    int total = -1;
                    int skipped = -1;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (Clusters.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successful = parser.intValue();
                            } else if (Clusters.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                total = parser.intValue();
                            } else if (Clusters.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skipped = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                    clusters = new Clusters(total, successful, skipped);
                } else {
                    parser.skipChildren();
                }
            }
        }
        SearchResponseSections searchResponseSections = new SearchResponseSections(
            hits,
            aggs,
            suggest,
            timedOut,
            terminatedEarly,
            profile,
            numReducePhases
        );
        return new SearchResponse(
            searchResponseSections,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            failures.toArray(ShardSearchFailure.EMPTY_ARRAY),
            clusters,
            searchContextId
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        internalResponse.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);

        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }
        clusters.writeTo(out);
        out.writeOptionalString(scrollId);
        out.writeVLong(tookInMillis);
        out.writeVInt(skippedShards);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            out.writeOptionalString(pointInTimeId);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Holds info about the clusters that the search was executed on: how many in total, how many of them were successful
     * and how many of them were skipped.
     */
    public static class Clusters implements ToXContentFragment, Writeable {

        public static final Clusters EMPTY = new Clusters(0, 0, 0);

        static final ParseField _CLUSTERS_FIELD = new ParseField("_clusters");
        static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
        static final ParseField SKIPPED_FIELD = new ParseField("skipped");
        static final ParseField TOTAL_FIELD = new ParseField("total");

        private final int total;
        private final int successful;
        private final int skipped;

        public Clusters(int total, int successful, int skipped) {
            assert total >= 0 && successful >= 0 && skipped >= 0
                : "total: " + total + " successful: " + successful + " skipped: " + skipped;
            assert successful <= total && skipped == total - successful
                : "total: " + total + " successful: " + successful + " skipped: " + skipped;
            this.total = total;
            this.successful = successful;
            this.skipped = skipped;
        }

        private Clusters(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVInt(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(skipped);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (total > 0) {
                builder.startObject(_CLUSTERS_FIELD.getPreferredName());
                builder.field(TOTAL_FIELD.getPreferredName(), total);
                builder.field(SUCCESSFUL_FIELD.getPreferredName(), successful);
                builder.field(SKIPPED_FIELD.getPreferredName(), skipped);
                builder.endObject();
            }
            return builder;
        }

        /**
         * Returns how many total clusters the search was requested to be executed on
         */
        public int getTotal() {
            return total;
        }

        /**
         * Returns how many total clusters the search was executed successfully on
         */
        public int getSuccessful() {
            return successful;
        }

        /**
         * Returns how many total clusters were during the execution of the search request
         */
        public int getSkipped() {
            return skipped;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Clusters clusters = (Clusters) o;
            return total == clusters.total && successful == clusters.successful && skipped == clusters.skipped;
        }

        @Override
        public int hashCode() {
            return Objects.hash(total, successful, skipped);
        }

        @Override
        public String toString() {
            return "Clusters{total=" + total + ", successful=" + successful + ", skipped=" + skipped + '}';
        }
    }

    // public for tests
    public static SearchResponse empty(Supplier<Long> tookInMillisSupplier, Clusters clusters) {
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
            searchHits,
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            0
        );
        return new SearchResponse(
            internalSearchResponse,
            null,
            0,
            0,
            0,
            tookInMillisSupplier.get(),
            ShardSearchFailure.EMPTY_ARRAY,
            clusters,
            null
        );
    }
}
