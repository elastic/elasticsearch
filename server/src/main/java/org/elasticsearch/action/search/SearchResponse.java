/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;

/**
 * A response of a search request.
 */
public class SearchResponse extends ActionResponse implements ChunkedToXContentObject {

    // for cross-cluster scenarios where cluster names are shown in API responses, use this string
    // rather than empty string (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) we use internally
    public static final String LOCAL_CLUSTER_NAME_REPRESENTATION = "(local)";

    public static final ParseField SCROLL_ID = new ParseField("_scroll_id");
    public static final ParseField POINT_IN_TIME_ID = new ParseField("pit_id");
    public static final ParseField TOOK = new ParseField("took");
    public static final ParseField TIMED_OUT = new ParseField("timed_out");
    public static final ParseField TERMINATED_EARLY = new ParseField("terminated_early");
    public static final ParseField NUM_REDUCE_PHASES = new ParseField("num_reduce_phases");

    private final SearchHits hits;
    private final InternalAggregations aggregations;
    private final Suggest suggest;
    private final SearchProfileResults profileResults;
    private final boolean timedOut;
    private final Boolean terminatedEarly;
    private final int numReducePhases;
    private final String scrollId;
    private final BytesReference pointInTimeId;
    private final int totalShards;
    private final int successfulShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;
    private final Clusters clusters;
    private final long tookInMillis;

    private final RefCounted refCounted = LeakTracker.wrap(new SimpleRefCounted());

    public SearchResponse(StreamInput in) throws IOException {
        this.hits = SearchHits.readFrom(in, true);
        if (in.readBoolean()) {
            // deserialize the aggregations trying to deduplicate the object created
            // TODO: use DelayableWriteable instead.
            this.aggregations = InternalAggregations.readFrom(
                DelayableWriteable.wrapWithDeduplicatorStreamInput(in, in.getTransportVersion(), in.namedWriteableRegistry())
            );
        } else {
            this.aggregations = null;
        }
        this.suggest = in.readBoolean() ? new Suggest(in) : null;
        this.timedOut = in.readBoolean();
        this.terminatedEarly = in.readOptionalBoolean();
        this.profileResults = in.readOptionalWriteable(SearchProfileResults::new);
        this.numReducePhases = in.readVInt();
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
        pointInTimeId = in.readOptionalBytesReference();
    }

    public SearchResponse(
        SearchHits hits,
        InternalAggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileResults profileResults,
        int numReducePhases,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters
    ) {
        this(
            hits,
            aggregations,
            suggest,
            timedOut,
            terminatedEarly,
            profileResults,
            numReducePhases,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            shardFailures,
            clusters,
            null
        );
    }

    public SearchResponse(
        SearchResponseSections searchResponseSections,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        BytesReference pointInTimeId
    ) {
        this(
            searchResponseSections.hits,
            searchResponseSections.aggregations,
            searchResponseSections.suggest,
            searchResponseSections.timedOut,
            searchResponseSections.terminatedEarly,
            searchResponseSections.profileResults,
            searchResponseSections.numReducePhases,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            shardFailures,
            clusters,
            pointInTimeId
        );
    }

    public SearchResponse(
        SearchHits hits,
        InternalAggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileResults profileResults,
        int numReducePhases,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        BytesReference pointInTimeId
    ) {
        this.hits = hits;
        hits.incRef();
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.profileResults = profileResults;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
        this.numReducePhases = numReducePhases;
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
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        if (refCounted.decRef()) {
            hits.decRef();
            return true;
        }
        return false;
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }

    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }

    /**
     * The search hits.
     */
    public SearchHits getHits() {
        assert hasReferences();
        return hits;
    }

    /**
     * Aggregations in this response. "empty" aggregations could be
     * either {@code null} or {@link InternalAggregations#EMPTY}.
     */
    public @Nullable InternalAggregations getAggregations() {
        return aggregations;
    }

    /**
     * Will {@link #getAggregations()} return non-empty aggregation results?
     */
    public boolean hasAggregations() {
        return getAggregations() != null && getAggregations() != InternalAggregations.EMPTY;
    }

    public Suggest getSuggest() {
        return suggest;
    }

    /**
     * Has the search operation timed out.
     */
    public boolean isTimedOut() {
        return timedOut;
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    public Boolean isTerminatedEarly() {
        return terminatedEarly;
    }

    /**
     * Returns the number of reduce phases applied to obtain this search response
     */
    public int getNumReducePhases() {
        return numReducePhases;
    }

    /**
     * How long the search took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
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
     * If scrolling was enabled ({@link SearchRequest#scroll(TimeValue)}, the scroll id that can be used to continue scrolling.
     */
    public String getScrollId() {
        return scrollId;
    }

    /**
     * Returns the encoded string of the search context that the search request is used to executed
     */
    public BytesReference pointInTimeId() {
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
        if (profileResults == null) {
            return Collections.emptyMap();
        }
        return profileResults.getShardResults();
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
        assert hasReferences();
        return getToXContentIterator(true, params);
    }

    public Iterator<? extends ToXContent> innerToXContentChunked(ToXContent.Params params) {
        return getToXContentIterator(false, params);
    }

    private Iterator<ToXContent> getToXContentIterator(boolean wrapInObject, ToXContent.Params params) {
        return Iterators.concat(
            wrapInObject ? ChunkedToXContentHelper.startObject() : Collections.emptyIterator(),
            ChunkedToXContentHelper.chunk(SearchResponse.this::headerToXContent),
            Iterators.single(clusters),
            hits.toXContentChunked(params),
            aggregations == null ? Collections.emptyIterator() : ChunkedToXContentHelper.chunk(aggregations),
            suggest == null ? Collections.emptyIterator() : ChunkedToXContentHelper.chunk(suggest),
            profileResults == null ? Collections.emptyIterator() : ChunkedToXContentHelper.chunk(profileResults),
            wrapInObject ? ChunkedToXContentHelper.endObject() : Collections.emptyIterator()
        );
    }

    public XContentBuilder headerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (scrollId != null) {
            builder.field(SCROLL_ID.getPreferredName(), scrollId);
        }
        if (pointInTimeId != null) {
            builder.field(
                POINT_IN_TIME_ID.getPreferredName(),
                Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(pointInTimeId))
            );
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert hasReferences();
        hits.writeTo(out);
        out.writeOptionalWriteable(aggregations);
        out.writeOptionalWriteable(suggest);
        out.writeBoolean(timedOut);
        out.writeOptionalBoolean(terminatedEarly);
        out.writeOptionalWriteable(profileResults);
        out.writeVInt(numReducePhases);
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
        out.writeOptionalBytesReference(pointInTimeId);
    }

    @Override
    public String toString() {
        return hasReferences() == false ? "SearchResponse[released]" : Strings.toString(this);
    }

    /**
     * Holds info about the clusters that the search was executed on: how many in total, how many of them were successful
     * and how many of them were skipped and further details in a Map of Cluster objects
     * (when doing a cross-cluster search).
     */
    public static final class Clusters implements ToXContentFragment, Writeable {

        public static final Clusters EMPTY = new Clusters(0, 0, 0);

        public static final ParseField _CLUSTERS_FIELD = new ParseField("_clusters");
        public static final ParseField TOTAL_FIELD = new ParseField("total");
        public static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
        public static final ParseField SKIPPED_FIELD = new ParseField("skipped");
        public static final ParseField RUNNING_FIELD = new ParseField("running");
        public static final ParseField PARTIAL_FIELD = new ParseField("partial");
        public static final ParseField FAILED_FIELD = new ParseField("failed");
        public static final ParseField DETAILS_FIELD = new ParseField("details");

        private final int total;
        private final int successful;   // not used for minimize_roundtrips=true; dynamically determined from clusterInfo map
        private final int skipped;      // not used for minimize_roundtrips=true; dynamically determined from clusterInfo map

        // key to map is clusterAlias on the primary querying cluster of a CCS minimize_roundtrips=true query
        // the Map itself is immutable after construction - all Clusters will be accounted for at the start of the search
        // updates to the Cluster occur with the updateCluster method that given the key to map transforms an
        // old Cluster Object to a new Cluster Object with the remapping function.
        private final Map<String, Cluster> clusterInfo;

        // not Writeable since it is only needed on the (primary) CCS coordinator
        private final transient Boolean ccsMinimizeRoundtrips;

        /**
         * For use with cross-cluster searches.
         * When minimizing roundtrips, the number of successful, skipped, running, partial and failed clusters
         * is not known until the end of the search and it the information in SearchResponse.Cluster object
         * will be updated as each cluster returns.
         * @param localIndices The localIndices to be searched - null if no local indices are to be searched
         * @param remoteClusterIndices mapping of clusterAlias -> OriginalIndices for each remote cluster
         * @param ccsMinimizeRoundtrips whether minimizing roundtrips for the CCS
         * @param skipUnavailablePredicate given a cluster alias, returns true if that cluster is skip_unavailable=true
         *                                 and false otherwise
         */
        public Clusters(
            @Nullable OriginalIndices localIndices,
            Map<String, OriginalIndices> remoteClusterIndices,
            boolean ccsMinimizeRoundtrips,
            Predicate<String> skipUnavailablePredicate
        ) {
            assert remoteClusterIndices.size() > 0 : "At least one remote cluster must be passed into this Cluster constructor";
            this.total = remoteClusterIndices.size() + (localIndices == null ? 0 : 1);
            assert total >= 1 : "No local indices or remote clusters passed in";
            this.successful = 0; // calculated from clusterInfo map for minimize_roundtrips
            this.skipped = 0;    // calculated from clusterInfo map for minimize_roundtrips
            this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
            Map<String, Cluster> m = ConcurrentCollections.newConcurrentMap();
            if (localIndices != null) {
                String localKey = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
                Cluster c = new Cluster(localKey, String.join(",", localIndices.indices()), false);
                m.put(localKey, c);
            }
            for (Map.Entry<String, OriginalIndices> remote : remoteClusterIndices.entrySet()) {
                String clusterAlias = remote.getKey();
                boolean skipUnavailable = skipUnavailablePredicate.test(clusterAlias);
                Cluster c = new Cluster(clusterAlias, String.join(",", remote.getValue().indices()), skipUnavailable);
                m.put(clusterAlias, c);
            }
            this.clusterInfo = m;
        }

        /**
         * Used for searches that are either not cross-cluster.
         * For CCS minimize_roundtrips=true use {@code Clusters(OriginalIndices, Map<String, OriginalIndices>, boolean)}
         * @param total total number of clusters in the search
         * @param successful number of successful clusters in the search
         * @param skipped number of skipped clusters (skipped can only happen for remote clusters with skip_unavailable=true)
         */
        public Clusters(int total, int successful, int skipped) {
            // TODO: change assert to total == 1 or total = 0 - this should probably only be used for local searches now
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
            int successfulTemp = in.readVInt();
            int skippedTemp = in.readVInt();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
                List<Cluster> clusterList = in.readCollectionAsList(Cluster::new);
                if (clusterList.isEmpty()) {
                    this.clusterInfo = Collections.emptyMap();
                    this.successful = successfulTemp;
                    this.skipped = skippedTemp;
                } else {
                    Map<String, Cluster> m = ConcurrentCollections.newConcurrentMap();
                    clusterList.forEach(c -> m.put(c.getClusterAlias(), c));
                    this.clusterInfo = m;
                    this.successful = getClusterStateCount(Cluster.Status.SUCCESSFUL);
                    this.skipped = getClusterStateCount(Cluster.Status.SKIPPED);
                }
            } else {
                this.successful = successfulTemp;
                this.skipped = skippedTemp;
                this.clusterInfo = Collections.emptyMap();
            }
            int running = getClusterStateCount(Cluster.Status.RUNNING);
            int partial = getClusterStateCount(Cluster.Status.PARTIAL);
            int failed = getClusterStateCount(Cluster.Status.FAILED);
            this.ccsMinimizeRoundtrips = false;
            assert total >= 0 : "total is negative: " + total;
            assert total == successful + skipped + running + partial + failed
                : "successful + skipped + running + partial + failed is not equal to total. total: "
                    + total
                    + " successful: "
                    + successful
                    + " skipped: "
                    + skipped
                    + " running: "
                    + running
                    + " partial: "
                    + partial
                    + " failed: "
                    + failed;
        }

        public Clusters(Map<String, Cluster> clusterInfoMap) {
            assert clusterInfoMap.size() > 0 : "this constructor should not be called with an empty Cluster info map";
            this.total = clusterInfoMap.size();
            this.clusterInfo = clusterInfoMap;
            this.successful = getClusterStateCount(Cluster.Status.SUCCESSFUL);
            this.skipped = getClusterStateCount(Cluster.Status.SKIPPED);
            // should only be called if "details" section of fromXContent is present (for ccsMinimizeRoundtrips)
            this.ccsMinimizeRoundtrips = true;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(skipped);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
                if (clusterInfo != null) {
                    List<Cluster> clusterList = clusterInfo.values().stream().toList();
                    out.writeCollection(clusterList);
                } else {
                    out.writeCollection(Collections.emptyList());
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (total > 0) {
                builder.startObject(_CLUSTERS_FIELD.getPreferredName());
                builder.field(TOTAL_FIELD.getPreferredName(), total);
                builder.field(SUCCESSFUL_FIELD.getPreferredName(), getClusterStateCount(Cluster.Status.SUCCESSFUL));
                builder.field(SKIPPED_FIELD.getPreferredName(), getClusterStateCount(Cluster.Status.SKIPPED));
                builder.field(RUNNING_FIELD.getPreferredName(), getClusterStateCount(Cluster.Status.RUNNING));
                builder.field(PARTIAL_FIELD.getPreferredName(), getClusterStateCount(Cluster.Status.PARTIAL));
                builder.field(FAILED_FIELD.getPreferredName(), getClusterStateCount(Cluster.Status.FAILED));
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
         * @param status the state you want to query
         * @return how many clusters are currently in a specific state
         */
        public int getClusterStateCount(Cluster.Status status) {
            if (clusterInfo.isEmpty()) {
                return switch (status) {
                    case SUCCESSFUL -> successful;
                    case SKIPPED -> skipped;
                    default -> 0;
                };
            } else {
                return determineCountFromClusterInfo(cluster -> cluster.getStatus() == status);
            }
        }

        /**
         * When Clusters is using the clusterInfo map (and Cluster objects are being updated in various
         * ActionListener threads), this method will count how many clusters match the passed in predicate.
         *
         * @param predicate to evaluate
         * @return count of clusters matching the predicate
         */
        private int determineCountFromClusterInfo(Predicate<Cluster> predicate) {
            return (int) clusterInfo.values().stream().filter(predicate).count();
        }

        /**
         * @return whether this search was a cross cluster search done with ccsMinimizeRoundtrips=true
         */
        public Boolean isCcsMinimizeRoundtrips() {
            return ccsMinimizeRoundtrips;
        }

        /**
         * @param clusterAlias The cluster alias as specified in the cluster collection
         * @return Cluster object associated with teh clusterAlias or null if not present
         */
        public Cluster getCluster(String clusterAlias) {
            return clusterInfo.get(clusterAlias);
        }

        /**
         * @return collection of cluster aliases in the search response (including "(local)" if was searched).
         */
        public Set<String> getClusterAliases() {
            return clusterInfo.keySet();
        }

        /**
         * Utility to swap a Cluster object. Guidelines for the remapping function:
         * <ul>
         * <li> The remapping function should return a new Cluster object to swap it for
         * the existing one.</li>
         * <li> If in the remapping function you decide to abort the swap you must return
         * the original Cluster object to keep the map unchanged.</li>
         * <li> Do not return {@code null}. If the remapping function returns {@code null},
         * the mapping is removed (or remains absent if initially absent).</li>
         * <li> If the remapping function itself throws an (unchecked) exception, the exception
         * is rethrown, and the current mapping is left unchanged. Throwing exception therefore
         * is OK, but it is generally discouraged.</li>
         * <li> The remapping function may be called multiple times in a CAS fashion underneath,
         * make sure that is safe to do so.</li>
         * </ul>
         * @param clusterAlias key with which the specified value is associated
         * @param remappingFunction function to swap the oldCluster to a newCluster
         * @return the new Cluster object
         */
        public Cluster swapCluster(String clusterAlias, BiFunction<String, Cluster, Cluster> remappingFunction) {
            return clusterInfo.compute(clusterAlias, remappingFunction);
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
            return total == clusters.total
                && getClusterStateCount(Cluster.Status.SUCCESSFUL) == clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL)
                && getClusterStateCount(Cluster.Status.SKIPPED) == clusters.getClusterStateCount(Cluster.Status.SKIPPED)
                && getClusterStateCount(Cluster.Status.RUNNING) == clusters.getClusterStateCount(Cluster.Status.RUNNING)
                && getClusterStateCount(Cluster.Status.PARTIAL) == clusters.getClusterStateCount(Cluster.Status.PARTIAL)
                && getClusterStateCount(Cluster.Status.FAILED) == clusters.getClusterStateCount(Cluster.Status.FAILED);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                total,
                getClusterStateCount(Cluster.Status.SUCCESSFUL),
                getClusterStateCount(Cluster.Status.SKIPPED),
                getClusterStateCount(Cluster.Status.RUNNING),
                getClusterStateCount(Cluster.Status.PARTIAL),
                getClusterStateCount(Cluster.Status.FAILED)
            );
        }

        @Override
        public String toString() {
            return "Clusters{total="
                + total
                + ", successful="
                + getClusterStateCount(Cluster.Status.SUCCESSFUL)
                + ", skipped="
                + getClusterStateCount(Cluster.Status.SKIPPED)
                + ", running="
                + getClusterStateCount(Cluster.Status.RUNNING)
                + ", partial="
                + getClusterStateCount(Cluster.Status.PARTIAL)
                + ", failed="
                + getClusterStateCount(Cluster.Status.FAILED)
                + '}';
        }

        /**
         * @return true if any underlying Cluster objects have PARTIAL, SKIPPED, FAILED or RUNNING status.
         *              or any Cluster is marked as timedOut.
         */
        public boolean hasPartialResults() {
            for (Cluster cluster : clusterInfo.values()) {
                switch (cluster.getStatus()) {
                    case PARTIAL, SKIPPED, FAILED, RUNNING -> {
                        return true;
                    }
                }
                if (cluster.isTimedOut()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @return true if this Clusters object was initialized with underlying Cluster objects
         * for tracking search Cluster details.
         */
        public boolean hasClusterObjects() {
            return clusterInfo.isEmpty() == false;
        }

        /**
         * @return true if this Clusters object has been initialized with remote Cluster objects
         *              This will be false for local-cluster (non-CCS) only searches.
         */
        public boolean hasRemoteClusters() {
            return total > 1
                || clusterInfo.keySet().stream().anyMatch(alias -> alias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false);
        }

    }

    /**
     * Represents the search metadata about a particular cluster involved in a cross-cluster search.
     * The Cluster object can represent either the local cluster or a remote cluster.
     * For the local cluster, clusterAlias should be specified as RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.
     * Its XContent is put into the "details" section the "_clusters" entry in the SearchResponse.
     * This is an immutable class, so updates made during the search progress (especially important for async
     * CCS searches) must be done by replacing the Cluster object with a new one.
     * See the Clusters clusterInfo Map for details.
     */
    public static class Cluster implements ToXContentFragment, Writeable {
        public static final ParseField INDICES_FIELD = new ParseField("indices");
        public static final ParseField STATUS_FIELD = new ParseField("status");

        public static final boolean SKIP_UNAVAILABLE_DEFAULT = false;

        private final String clusterAlias;
        private final String indexExpression; // original index expression from the user for this cluster
        private final boolean skipUnavailable;
        private final Status status;
        private final Integer totalShards;
        private final Integer successfulShards;
        private final Integer skippedShards;
        private final Integer failedShards;
        private final List<ShardSearchFailure> failures;
        private final TimeValue took;  // search latency in millis for this cluster sub-search
        private final boolean timedOut;

        /**
         * Marks the status of a Cluster search involved in a Cross-Cluster search.
         */
        public enum Status {
            RUNNING,     // still running
            SUCCESSFUL,  // all shards completed search
            PARTIAL,     // only some shards completed the search, partial results from cluster
            SKIPPED,     // entire cluster was skipped
            FAILED;      // search was failed due to errors on this cluster

            @Override
            public String toString() {
                return this.name().toLowerCase(Locale.ROOT);
            }
        }

        /**
         * Create a Cluster object representing the initial RUNNING state of a Cluster.
         *
         * @param clusterAlias clusterAlias as defined in the remote cluster settings or RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY
         *                     for the local cluster
         * @param indexExpression the original (not resolved/concrete) indices expression provided for this cluster.
         * @param skipUnavailable whether this Cluster is marked as skip_unavailable in remote cluster settings
         */
        public Cluster(String clusterAlias, String indexExpression, boolean skipUnavailable) {
            this(clusterAlias, indexExpression, skipUnavailable, Status.RUNNING, null, null, null, null, null, null, false);
        }

        public Cluster(
            String clusterAlias,
            String indexExpression,
            boolean skipUnavailable,
            Status status,
            Integer totalShards,
            Integer successfulShards,
            Integer skippedShards,
            Integer failedShards,
            List<ShardSearchFailure> failures,
            TimeValue took,
            boolean timedOut
        ) {
            assert clusterAlias != null : "clusterAlias cannot be null";
            assert indexExpression != null : "indexExpression of Cluster cannot be null";
            assert status != null : "status of Cluster cannot be null";
            this.clusterAlias = clusterAlias;
            this.indexExpression = indexExpression;
            this.skipUnavailable = skipUnavailable;
            this.status = status;
            this.totalShards = totalShards;
            this.successfulShards = successfulShards;
            this.skippedShards = skippedShards;
            this.failedShards = failedShards;
            this.failures = failures == null ? Collections.emptyList() : Collections.unmodifiableList(failures);
            this.took = took;
            this.timedOut = timedOut;
        }

        public Cluster(StreamInput in) throws IOException {
            this.clusterAlias = in.readString();
            this.indexExpression = in.readString();
            this.status = Status.valueOf(in.readString().toUpperCase(Locale.ROOT));
            this.totalShards = in.readOptionalInt();
            this.successfulShards = in.readOptionalInt();
            this.skippedShards = in.readOptionalInt();
            this.failedShards = in.readOptionalInt();
            Long took = in.readOptionalLong();
            if (took == null) {
                this.took = null;
            } else {
                this.took = new TimeValue(took);
            }
            this.timedOut = in.readBoolean();
            this.failures = Collections.unmodifiableList(in.readCollectionAsList(ShardSearchFailure::readShardSearchFailure));
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
                this.skipUnavailable = in.readBoolean();
            } else {
                this.skipUnavailable = SKIP_UNAVAILABLE_DEFAULT;
            }
        }

        /**
         * Since the Cluster object is immutable, use this Builder class to create
         * a new Cluster object using the "copyFrom" Cluster passed in and set only
         * changed values.
         *
         * Since the clusterAlias, indexExpression and skipUnavailable fields are
         * never changed once set, this Builder provides no setter method for them.
         * All other fields can be set and override the value in the "copyFrom" Cluster.
         */
        public static class Builder {
            private Status status;
            private Integer totalShards;
            private Integer successfulShards;
            private Integer skippedShards;
            private Integer failedShards;
            private List<ShardSearchFailure> failures;
            private TimeValue took;
            private Boolean timedOut;
            private final Cluster original;

            public Builder(Cluster copyFrom) {
                this.original = copyFrom;
            }

            /**
             * @return new Cluster object using the new values passed in via setters
             *         or the values in the "copyFrom" Cluster object set in the
             *         Builder constructor.
             */
            public Cluster build() {
                return new Cluster(
                    original.getClusterAlias(),
                    original.getIndexExpression(),
                    original.isSkipUnavailable(),
                    status != null ? status : original.getStatus(),
                    totalShards != null ? totalShards : original.getTotalShards(),
                    successfulShards != null ? successfulShards : original.getSuccessfulShards(),
                    skippedShards != null ? skippedShards : original.getSkippedShards(),
                    failedShards != null ? failedShards : original.getFailedShards(),
                    failures != null ? failures : original.getFailures(),
                    took != null ? took : original.getTook(),
                    timedOut != null ? timedOut : original.isTimedOut()
                );
            }

            public Builder setStatus(Status status) {
                this.status = status;
                return this;
            }

            public Builder setTotalShards(int totalShards) {
                this.totalShards = totalShards;
                return this;
            }

            public Builder setSuccessfulShards(int successfulShards) {
                this.successfulShards = successfulShards;
                return this;
            }

            public Builder setSkippedShards(int skippedShards) {
                this.skippedShards = skippedShards;
                return this;
            }

            public Builder setFailedShards(int failedShards) {
                this.failedShards = failedShards;
                return this;
            }

            public Builder setFailures(List<ShardSearchFailure> failures) {
                this.failures = failures;
                return this;
            }

            public Builder setTook(TimeValue took) {
                this.took = took;
                return this;
            }

            public Builder setTimedOut(boolean timedOut) {
                this.timedOut = timedOut;
                return this;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterAlias);
            out.writeString(indexExpression);
            out.writeString(status.toString());
            out.writeOptionalInt(totalShards);
            out.writeOptionalInt(successfulShards);
            out.writeOptionalInt(skippedShards);
            out.writeOptionalInt(failedShards);
            out.writeOptionalLong(took == null ? null : took.millis());
            out.writeBoolean(timedOut);
            out.writeCollection(failures);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
                out.writeBoolean(skipUnavailable);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String name = clusterAlias;
            if (clusterAlias.isEmpty()) {
                name = LOCAL_CLUSTER_NAME_REPRESENTATION;
            }
            builder.startObject(name);
            {
                builder.field(STATUS_FIELD.getPreferredName(), getStatus().toString());
                builder.field(INDICES_FIELD.getPreferredName(), indexExpression);
                if (took != null) {
                    builder.field(TOOK.getPreferredName(), took.millis());
                }
                builder.field(TIMED_OUT.getPreferredName(), timedOut);
                if (totalShards != null) {
                    builder.startObject(RestActions._SHARDS_FIELD.getPreferredName());
                    builder.field(RestActions.TOTAL_FIELD.getPreferredName(), totalShards);
                    if (successfulShards != null) {
                        builder.field(RestActions.SUCCESSFUL_FIELD.getPreferredName(), successfulShards);
                    }
                    if (skippedShards != null) {
                        builder.field(RestActions.SKIPPED_FIELD.getPreferredName(), skippedShards);
                    }
                    if (failedShards != null) {
                        builder.field(RestActions.FAILED_FIELD.getPreferredName(), failedShards);
                    }
                    builder.endObject();
                }
                if (failures != null && failures.size() > 0) {
                    builder.startArray(RestActions.FAILURES_FIELD.getPreferredName());
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

        public String getIndexExpression() {
            return indexExpression;
        }

        public boolean isSkipUnavailable() {
            return skipUnavailable;
        }

        public Status getStatus() {
            return status;
        }

        public boolean isTimedOut() {
            return timedOut;
        }

        public List<ShardSearchFailure> getFailures() {
            return failures;
        }

        public TimeValue getTook() {
            return took;
        }

        public Integer getTotalShards() {
            return totalShards;
        }

        public Integer getSuccessfulShards() {
            return successfulShards;
        }

        public Integer getSkippedShards() {
            return skippedShards;
        }

        public Integer getFailedShards() {
            return failedShards;
        }

        @Override
        public String toString() {
            return "Cluster{"
                + "alias='"
                + clusterAlias
                + '\''
                + ", status="
                + status
                + ", totalShards="
                + totalShards
                + ", successfulShards="
                + successfulShards
                + ", skippedShards="
                + skippedShards
                + ", failedShards="
                + failedShards
                + ", failures(sz)="
                + failures.size()
                + ", took="
                + took
                + ", timedOut="
                + timedOut
                + ", indexExpression='"
                + indexExpression
                + '\''
                + ", skipUnavailable="
                + skipUnavailable
                + '}';
        }
    }

    // public for tests
    public static SearchResponse empty(Supplier<Long> tookInMillisSupplier, Clusters clusters) {
        return new SearchResponse(
            SearchHits.empty(Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN),
            InternalAggregations.EMPTY,
            null,
            false,
            null,
            null,
            0,
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
