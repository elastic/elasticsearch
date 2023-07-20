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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
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
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response of a search request.
 */
public class SearchResponse extends ActionResponse implements ChunkedToXContentObject {

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
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            this.innerToXContentChunked(params),
            ChunkedToXContentHelper.endObject()
        );
    }

    public Iterator<? extends ToXContent> innerToXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.singleChunk(SearchResponse.this::headerToXContent),
            Iterators.single(clusters),
            internalResponse.toXContentChunked(params)
        );
    }

    public XContentBuilder headerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
        private final int successful;  // not used for minimize_roundtrips=true; dynamically determined from clusterInfo map
        private final int skipped;  // not used for minimize_roundtrips=true; dynamically determined from clusterInfo map

        // key to map is clusterAlias on the primary querying cluster of a CCS minimize_roundtrips=true query
        private final Map<String, Cluster> clusterInfo;

        // this field is not Writeable, as it is only needed on the initial "querying cluster" coordinator of a CCS search
        private final transient boolean ccsMinimizeRoundtrips;

        /**
         * A Clusters object meant for use with CCS holding additional information about
         * the number of remote clusters and whether ccsMinimizeRoundtrips is being used.
         * @param total total number of clusters in the search
         * @param successful number of clusters that have successfully completed the search
         * @param skipped number of clusters that were skipped (e.g., unavailable or other error)
         * @param remoteClusters number of remote clusters in the search
         * @param ccsMinimizeRoundtrips specifies whether a CCS search is using minimizeRoundtrips feature
         */
        @Deprecated  // for CCS use the Clusters(Collection<String>, boolean) constructor
        public Clusters(int total, int successful, int skipped, int remoteClusters, boolean ccsMinimizeRoundtrips) {
            assert total >= 0 && successful >= 0 && skipped >= 0 && remoteClusters >= 0
                : "total: " + total + " successful: " + successful + " skipped: " + skipped + " remote: " + remoteClusters;
            assert successful <= total : "total: " + total + " successful: " + successful + " skipped: " + skipped;
            assert remoteClusters <= total : "total: " + total + " remote: " + remoteClusters;
            assert ccsMinimizeRoundtrips == false || remoteClusters > 0
                : "ccsMinimizeRoundtrips is true but remoteClusters count is not a positive number: " + remoteClusters;
            int localCount = total - remoteClusters;
            assert localCount == 0 || localCount == 1 : "total - remoteClusters should only be 0 or 1";
            this.total = total;
            this.successful = successful;
            this.skipped = skipped;
            this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
            this.clusterInfo = new ConcurrentHashMap<>();
        }

        /**
         * For use with cross-cluster searches with ccs_minimize_roundtrips=true.
         * When minimizing roundtrips, the number of successful and skipped clusters is not known until
         * the end of the search and it the information in SearchResponse.Cluster object will be updated
         * as each cluster returns.
         * @param clusterAliases
         * @param ccsMinimizeRoundtrips
         */
        public Clusters(Collection<String> clusterAliases, boolean ccsMinimizeRoundtrips) {
            this.total = clusterAliases.size();
            assert total >= 1 : "Cluster aliases collection passed in has size 0";
            this.successful = 0; // calculated from clusterInfo map for minimize_roundtrips
            this.skipped = 0;    // calculated from clusterInfo map for minimize_roundtrips
            this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
            this.clusterInfo = new ConcurrentHashMap<>();
            for (String clusterAlias : clusterAliases) {
                clusterInfo.put(clusterAlias, new Cluster(clusterAlias));
            }
        }

        /**
         * Used for searches that are either not cross-cluster or CCS with minimize_roundtrips=false.
         * For CCS minimize_roundtrips=true use {@code Clusters(Collection<String>, boolean)}.
         * @param total total number of clusters in the search
         * @param successful number of successful clusters in the search
         * @param skipped number of skipped clusters (skipped can only happen for remote clusters with skip_unavailable=true)
         */
        public Clusters(int total, int successful, int skipped) {
            assert total >= 0 && successful >= 0 && skipped >= 0 && successful <= total
                : "total: " + total + " successful: " + successful + " skipped: " + skipped;
            assert skipped == total - successful : "total: " + total + " successful: " + successful + " skipped: " + skipped;
            this.total = total;
            this.successful = successful;
            this.skipped = skipped;
            this.ccsMinimizeRoundtrips = false;
            this.clusterInfo = Collections.emptyMap();  // will never be used if created from this constructor
        }

        public Clusters(StreamInput in) throws IOException {
            this.total = in.readVInt();
            this.successful = in.readVInt();
            this.skipped = in.readVInt();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_053)) {
                this.clusterInfo = in.readMapValues(Cluster::new, Cluster::getClusterAlias);
            } else {
                this.clusterInfo = Collections.emptyMap();
            }
            this.ccsMinimizeRoundtrips = false;
            assert total >= 0 : "total is negative: " + total;
            assert total >= successful + skipped
                : "successful + skipped is larger than total. total: " + total + " successful: " + successful + " skipped: " + skipped;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(skipped);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_053)) {
                out.writeMapValues(clusterInfo);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (total > 0) {
                builder.startObject(_CLUSTERS_FIELD.getPreferredName());
                builder.field(TOTAL_FIELD.getPreferredName(), total);
                builder.field(SUCCESSFUL_FIELD.getPreferredName(), getSuccessful());
                builder.field(SKIPPED_FIELD.getPreferredName(), getSkipped());
                if (clusterInfo.size() > 0) {
                    builder.startObject("details");
                    for (Cluster cluster : clusterInfo.values()) {
                        cluster.toXContent(builder, params);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder;
        }

        /**
         * @return how many total clusters the search was requested to be executed on
         */
        public int getTotal() {
            return total;
        }

        /**
         * @return how many total clusters the search was executed successfully on
         */
        public int getSuccessful() {
            if (clusterInfo.isEmpty()) {
                return successful;
            } else {
                return (int) clusterInfo.values()
                    .stream()
                    .filter(c -> c.getStatus() == Cluster.Status.SUCCESSFUL || c.getStatus() == Cluster.Status.PARTIAL)
                    .count();
            }
        }

        /**
         * @return how many total clusters were used during the execution of the search request
         */
        public int getSkipped() {
            if (clusterInfo.isEmpty()) {
                return skipped;
            } else {
                return (int) clusterInfo.values()
                    .stream()
                    /// MP TODO: do we want a separate counter and XContent field for FAILED?
                    .filter(c -> c.getStatus() == Cluster.Status.SKIPPED || c.getStatus() == Cluster.Status.FAILED)
                    .count();
            }
        }

        /**
         * @return whether this search was a cross cluster search done with ccsMinimizeRoundtrips=true
         */
        public boolean isCcsMinimizeRoundtrips() {
            return ccsMinimizeRoundtrips;
        }

        /**
         * @param clusterAlias The cluster alias as specified in the cluster collection
         * @return Cluster object associated with teh clusterAlias or null if not present
         */
        public Cluster getCluster(String clusterAlias) {
            return clusterInfo.get(clusterAlias);
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
            /// MP TODO: not sure what to do about successful and skipped here, since they are calculated differently
            /// MP TODO: between CCS MRT=true and other search types
            return total == clusters.total && successful == clusters.successful && skipped == clusters.skipped;
        }

        @Override
        public int hashCode() {
            /// MP TODO: not sure what to do about successful and skipped here, since they are calculated differently
            /// MP TODO: between CCS MRT=true and other search types
            return Objects.hash(total, successful, skipped);
        }

        @Override
        public String toString() {
            return "Clusters{total=" + total + ", successful=" + getSuccessful() + ", skipped=" + getSkipped() + '}';
        }

        /**
         * @return true if any underlying Cluster objects have PARTIAL, SKIPPED, FAILED or RUNNING status.
         */
        public boolean hasPartialResults() {
            for (Cluster cluster : clusterInfo.values()) {
                switch (cluster.getStatus()) {
                    case PARTIAL:
                    case SKIPPED:
                    case FAILED:
                    case RUNNING:
                        return true;
                }
            }
            return false;
        }
    }

    /**
     * Represents the search metadata about a particular cluster involved in a cross-cluster search.
     * The Cluster object can represent both the local cluster and a remote cluster.
     * For the local cluster, clusterAlias should be specified as RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.
     * Its XContent is put into the "details" section the "_clusters" entry in the SearchResponse.
     * This is not an immutable class, since it needs to be updated as the search progress
     * (especially important for async CCS searches).
     */
    public static class Cluster implements ToXContentFragment, Writeable {
        private final String clusterAlias;
        private Status status;
        private final List<ShardSearchFailure> failures;
        private Integer totalShards;  /// MP TODO: use these to update the _shards fields (otherwise skipped clusters aren't counted)
        private Integer successfulShards;
        private Integer skippedShards;
        private Integer failedShards;
        private Long searchLatencyMillis;

        /**
         * Marks the status of a Cluster search involved in a Cross-Cluster search.
         */
        public enum Status {
            RUNNING,  // still running
            SUCCESSFUL,  // all shards completed search
            PARTIAL,  // only some shards completed the search, partial results from cluster
            SKIPPED,  // entire cluster was skipped
            FAILED;   // search was failed due to errors on this cluster

            @Override
            public String toString() {
                return this.name().toLowerCase(Locale.ROOT);
            }
        }

        public Cluster(String clusterAlias) {
            this.clusterAlias = clusterAlias;
            this.failures = new ArrayList<>();
            this.status = Status.RUNNING;
        }

        public Cluster(StreamInput in) throws IOException {
            this.clusterAlias = in.readString();
            this.status = Status.valueOf(in.readString().toUpperCase(Locale.ROOT));
            this.totalShards = in.readOptionalVInt();
            this.successfulShards = in.readOptionalVInt();
            this.skippedShards = in.readOptionalVInt();
            this.failedShards = in.readOptionalVInt();
            this.searchLatencyMillis = in.readOptionalVLong();
            this.failures = in.readList(ShardSearchFailure::readShardSearchFailure);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterAlias);
            out.writeString(status.toString());
            out.writeOptionalVInt(totalShards);
            out.writeOptionalVInt(successfulShards);
            out.writeOptionalVInt(skippedShards);
            out.writeOptionalVInt(failedShards);
            out.writeOptionalVLong(searchLatencyMillis);
            out.writeList(failures);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String name = clusterAlias;
            if (clusterAlias.equals("")) {
                name = "(local)";
            }
            builder.startObject(name);
            {
                builder.field("status", status.toString());
                if (totalShards != null) {
                    builder.field("total_shards", totalShards);
                }
                if (successfulShards != null) {
                    builder.field("successful_shards", successfulShards);
                }
                if (skippedShards != null) {
                    builder.field("skipped_shards", skippedShards);
                }
                if (failedShards != null) {
                    builder.field("failed_shards", failedShards);
                }
                if (totalShards != null && successfulShards != null) {
                    String percentSuccessful = Strings.format("%.2f", (successfulShards.floatValue() / totalShards.floatValue()) * 100);
                    builder.field("percent_shards_successful", percentSuccessful);
                }
                if (searchLatencyMillis != null) {
                    builder.field("search_duration", searchLatencyMillis.doubleValue());
                }
                if (failures != null && failures.isEmpty() == false) {
                    builder.startArray("errors");
                    for (ShardSearchFailure failure : failures) {
                        failure.toXContent(builder, params);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
            return builder;
        }

        public String getClusterAlias() {
            return clusterAlias;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public List<ShardSearchFailure> getFailures() {
            return failures;
        }

        public void addFailure(ShardSearchFailure f) {
            this.failures.add(f);
        }

        public Long getSearchLatencyMillis() {
            return searchLatencyMillis;
        }

        public void setSearchLatencyMillis(Long searchLatencyMillis) {
            this.searchLatencyMillis = searchLatencyMillis;
        }

        public Integer getTotalShards() {
            return totalShards;
        }

        public void setTotalShards(int totalShards) {
            this.totalShards = totalShards;
        }

        public Integer getSuccessfulShards() {
            return successfulShards;
        }

        public void setSuccessfulShards(int successfulShards) {
            this.successfulShards = successfulShards;
        }

        public Integer getSkippedShards() {
            return skippedShards;
        }

        public void setSkippedShards(int skippedShards) {
            this.skippedShards = skippedShards;
        }

        public Integer getFailedShards() {
            return failedShards;
        }

        public void setFailedShards(int failedShards) {
            this.failedShards = failedShards;
        }

        @Override
        public String toString() {
            return "Cluster{"
                + "clusterAlias='"
                + clusterAlias
                + '\''
                + ", status="
                + status
                + ", failures="
                + failures
                + ", totalShards="
                + totalShards
                + ", successfulShards="
                + successfulShards
                + ", skippedShards="
                + skippedShards
                + ", failedShards="
                + failedShards
                + ", searchLatencyMillis="
                + searchLatencyMillis
                + '}';
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
