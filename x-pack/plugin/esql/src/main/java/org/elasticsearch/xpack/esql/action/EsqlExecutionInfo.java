/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Holds execution metadata about ES|QL queries for cross-cluster searches in order to display
 * this information in ES|QL JSON responses.
 * Patterned after the SearchResponse.Clusters and SearchResponse.Cluster classes.
 */
public class EsqlExecutionInfo implements ChunkedToXContentObject, Writeable {
    // for cross-cluster scenarios where cluster names are shown in API responses, use this string
    // rather than empty string (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) we use internally
    public static final String LOCAL_CLUSTER_NAME_REPRESENTATION = "(local)";

    public static final ParseField TOTAL_FIELD = new ParseField("total");
    public static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
    public static final ParseField SKIPPED_FIELD = new ParseField("skipped");
    public static final ParseField RUNNING_FIELD = new ParseField("running");
    public static final ParseField PARTIAL_FIELD = new ParseField("partial");
    public static final ParseField FAILED_FIELD = new ParseField("failed");
    public static final ParseField DETAILS_FIELD = new ParseField("details");
    public static final ParseField TOOK = new ParseField("took");
    public static final ParseField IS_PARTIAL_FIELD = new ParseField("is_partial");

    private static final TransportVersion ESQL_QUERY_PLANNING_DURATION = TransportVersion.fromName("esql_query_planning_duration");
    public static final TransportVersion EXECUTION_METADATA_VERSION = TransportVersion.fromName("esql_execution_metadata");

    // Map key is clusterAlias on the primary querying cluster of a CCS minimize_roundtrips=true query
    // The Map itself is immutable after construction - all Clusters will be accounted for at the start of the search.
    // Updates to the Cluster occur with the updateCluster method that given the key to map transforms an
    // old Cluster Object to a new Cluster Object with the remapping function.
    public final ConcurrentMap<String, Cluster> clusterInfo;
    // Is the clusterInfo map iinitialization in progress? If so, we should not try to serialize it.
    private transient volatile boolean clusterInfoInitializing;

    public enum IncludeExecutionMetadata {
        ALWAYS,
        CCS_ONLY,
        NEVER
    }

    // whether the user has asked for execution/CCS metadata to be in the JSON response (the overall took will always be present)
    private final IncludeExecutionMetadata includeExecutionMetadata;

    // fields that are not Writeable since they are only needed on the primary CCS coordinator
    private final transient Predicate<String> skipOnFailurePredicate; // Predicate to determine if we should skip a cluster on failure
    private volatile boolean isPartial; // Does this request have partial results?
    private transient volatile boolean isStopped; // Have we received stop command?

    // start time for the ESQL query for calculating time spans relative to the beginning of the query
    private final transient TimeSpan.Builder relativeStart;
    private transient TimeSpan overallTimeSpan;
    private transient TimeSpan planningTimeSpan; // time elapsed since start of query to calling ComputeService.execute
    private TimeValue overallTook;

    // Are we doing subplans? No need to serialize this because it is only relevant for the coordinator node.
    private transient boolean inSubplan = false;

    // FOR TESTS ONLY
    public EsqlExecutionInfo(boolean includeCCSMetadata) {
        // default all clusters to being skippable on failure
        this(Predicates.always(), includeCCSMetadata ? IncludeExecutionMetadata.CCS_ONLY : IncludeExecutionMetadata.NEVER);
    }

    /**
     * FOR TESTING use with fromXContent parsing ONLY
     */
    EsqlExecutionInfo(ConcurrentMap<String, Cluster> clusterInfo, boolean includeCCSMetadata) {
        this(
            clusterInfo,
            Predicates.always(),
            includeCCSMetadata ? IncludeExecutionMetadata.CCS_ONLY : IncludeExecutionMetadata.NEVER,
            null
        );
    }

    /**
     * @param skipOnPlanTimeFailurePredicate Decides whether we should skip the cluster that fails during planning phase.
     * @param includeExecutionMetadata (user defined setting) whether to include the execution/CCS metadata in the HTTP response
     */
    public EsqlExecutionInfo(Predicate<String> skipOnPlanTimeFailurePredicate, IncludeExecutionMetadata includeExecutionMetadata) {
        this(new ConcurrentHashMap<>(), skipOnPlanTimeFailurePredicate, includeExecutionMetadata, TimeSpan.start());
    }

    EsqlExecutionInfo(
        ConcurrentMap<String, Cluster> clusterInfo,
        Predicate<String> skipOnPlanTimeFailurePredicate,
        IncludeExecutionMetadata includeExecutionMetadata,
        TimeSpan.Builder relativeStart
    ) {
        assert includeExecutionMetadata != null;
        this.clusterInfo = clusterInfo;
        this.skipOnFailurePredicate = skipOnPlanTimeFailurePredicate;
        this.includeExecutionMetadata = includeExecutionMetadata;
        this.relativeStart = relativeStart;
    }

    public EsqlExecutionInfo(StreamInput in) throws IOException {
        this.overallTook = in.readOptionalTimeValue();
        this.clusterInfo = in.readMapValues(EsqlExecutionInfo.Cluster::new, Cluster::getClusterAlias, ConcurrentHashMap::new);
        if (in.getTransportVersion().supports(EXECUTION_METADATA_VERSION)) {
            this.includeExecutionMetadata = in.readEnum(IncludeExecutionMetadata.class);
        } else {
            this.includeExecutionMetadata = in.readBoolean() ? IncludeExecutionMetadata.CCS_ONLY : IncludeExecutionMetadata.NEVER;
        }
        this.isPartial = in.getTransportVersion().supports(TransportVersions.V_8_18_0) ? in.readBoolean() : false;
        this.skipOnFailurePredicate = Predicates.always();
        this.relativeStart = null;
        if (in.getTransportVersion().supports(ESQL_QUERY_PLANNING_DURATION)) {
            this.overallTimeSpan = in.readOptional(TimeSpan::readFrom);
            this.planningTimeSpan = in.readOptional(TimeSpan::readFrom);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(overallTook);
        if (clusterInfo != null && clusterInfoInitializing == false) {
            out.writeCollection(clusterInfo.values());
        } else {
            out.writeCollection(Collections.emptyList());
        }
        if (out.getTransportVersion().supports(EXECUTION_METADATA_VERSION)) {
            out.writeEnum(includeExecutionMetadata);
        } else {
            out.writeBoolean(includeExecutionMetadata != IncludeExecutionMetadata.NEVER);
        }
        if (out.getTransportVersion().supports(TransportVersions.V_8_18_0)) {
            out.writeBoolean(isPartial);
        }
        if (out.getTransportVersion().supports(ESQL_QUERY_PLANNING_DURATION)) {
            out.writeOptionalWriteable(overallTimeSpan);
            out.writeOptionalWriteable(planningTimeSpan);
        }
        assert inSubplan == false : "Should not be serializing execution info while in subplans";
    }

    // this is still here for testing only, use includeExecutionMetadata() in production code
    public boolean includeCCSMetadata() {
        return includeExecutionMetadata == IncludeExecutionMetadata.ALWAYS || includeExecutionMetadata == IncludeExecutionMetadata.CCS_ONLY;
    }

    public IncludeExecutionMetadata includeExecutionMetadata() {
        return includeExecutionMetadata;
    }

    /**
     * Call when ES|QL "planning" phase is complete and query execution (in ComputeService) is about to start.
     * Note this is currently only built for a single phase planning/execution model. When INLINE STATS
     * moves towards GA we may need to revisit this model. Currently, it should never be called more than once.
     */
    public void markEndPlanning() {
        assert planningTimeSpan == null : "markEndPlanning should only be called once";
        assert relativeStart != null : "Relative start time must be set when markEndPlanning is called";
        planningTimeSpan = relativeStart.stop();
    }

    public TimeValue planningTookTime() {
        return planningTimeSpan != null ? planningTimeSpan.toTimeValue() : null;
    }

    /**
     * Call when ES|QL execution is complete in order to set the overall took time for an ES|QL query.
     */
    public void markEndQuery() {
        assert relativeStart != null : "Relative start time must be set when markEndQuery is called";
        if (isMainPlan()) {
            overallTimeSpan = relativeStart.stop();
            overallTook = overallTimeSpan.toTimeValue();
        }
    }

    // for testing only - use markEndQuery in production code
    void overallTook(TimeValue took) {
        this.overallTook = took;
    }

    public TimeValue overallTook() {
        return overallTook;
    }

    /**
     * How much time the query took since starting.
     */
    public TimeValue tookSoFar() {
        return relativeStart != null ? relativeStart.stop().toTimeValue() : TimeValue.ZERO;
    }

    public TimeSpan overallTimeSpan() {
        return overallTimeSpan;
    }

    public TimeSpan planningTimeSpan() {
        return planningTimeSpan;
    }

    public Set<String> clusterAliases() {
        return clusterInfo.keySet();
    }

    /**
     * @param clusterAlias to check if we should skip this cluster on failure
     * @return whether it's OK to skip the cluster on failure.
     * @throws NoSuchRemoteClusterException if clusterAlias is unknown to this node's RemoteClusterService
     */
    public boolean shouldSkipOnFailure(String clusterAlias) {
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            // local cluster is not skippable for now
            return false;
        }
        return skipOnFailurePredicate.test(clusterAlias);
    }

    public boolean isCrossClusterSearch() {
        return clusterInfo.size() > 1
            || clusterInfo.size() == 1 && clusterInfo.containsKey(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY) == false;
    }

    /**
     * Is there any metadata to report in the response?
     * This is true on cross-cluster search with includeCCSMetadata=true or when there are partial failures.
     */
    public boolean hasMetadataToReport() {
        return includeExecutionMetadata == IncludeExecutionMetadata.ALWAYS && clusterInfo.isEmpty() == false
            || isCrossClusterSearch() && includeExecutionMetadata == IncludeExecutionMetadata.CCS_ONLY
            || (isPartial && clusterInfo.values().stream().anyMatch(c -> c.getFailures().isEmpty() == false));
    }

    public Cluster getCluster(String clusterAlias) {
        return clusterInfo.get(clusterAlias);
    }

    public Map<String, Cluster> getClusters() {
        return clusterInfo;
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
        return clusterInfo.compute(clusterAlias, (unused, oldCluster) -> {
            final Cluster newCluster = remappingFunction.apply(clusterAlias, oldCluster);
            if (newCluster != null && isPartial == false) {
                isPartial = newCluster.isPartial();
            }
            return newCluster;
        });
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // TODO remote this?
        if (clusterInfo.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (includeExecutionMetadata == IncludeExecutionMetadata.NEVER) {
            // If includeCCSMetadata is false, the only reason we're here is partial failures, so just report them.
            return onlyFailuresToXContent();
        }
        Map<Cluster.Status, Integer> clusterStatuses = new EnumMap<>(Cluster.Status.class);
        for (Cluster info : clusterInfo.values()) {
            clusterStatuses.merge(info.getStatus(), 1, Integer::sum);
        }
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.chunk(
                (b, p) -> b.field(TOTAL_FIELD.getPreferredName(), clusterInfo.size())
                    .field(SUCCESSFUL_FIELD.getPreferredName(), clusterStatuses.getOrDefault(Cluster.Status.SUCCESSFUL, 0))
                    .field(RUNNING_FIELD.getPreferredName(), clusterStatuses.getOrDefault(Cluster.Status.RUNNING, 0))
                    .field(SKIPPED_FIELD.getPreferredName(), clusterStatuses.getOrDefault(Cluster.Status.SKIPPED, 0))
                    .field(PARTIAL_FIELD.getPreferredName(), clusterStatuses.getOrDefault(Cluster.Status.PARTIAL, 0))
                    .field(FAILED_FIELD.getPreferredName(), clusterStatuses.getOrDefault(Cluster.Status.FAILED, 0))
            ),
            // each Cluster object defines its own field object name
            ChunkedToXContentHelper.object("details", clusterInfo.values().iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }

    private Iterator<? extends ToXContent> onlyFailuresToXContent() {
        Iterator<Cluster> failuresIterator = clusterInfo.values().stream().filter(c -> (c.getFailures().isEmpty() == false)).iterator();
        if (failuresIterator.hasNext()) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.object("details", failuresIterator),
                ChunkedToXContentHelper.endObject()
            );
        } else {
            return Collections.emptyIterator();
        }
    }

    /**
     * @param status the status you want to access
     * @return a stream of clusters with that status
     */
    public Stream<Cluster> getClusterStates(Cluster.Status status) {
        assert clusterInfo.isEmpty() == false : "ClusterMap in EsqlExecutionInfo must not be empty";
        return clusterInfo.values().stream().filter(cluster -> cluster.getStatus() == status);
    }

    public Stream<String> getRunningClusterAliases() {
        return getClusterStates(Cluster.Status.RUNNING).map(Cluster::getClusterAlias);
    }

    @Override
    public String toString() {
        return "EsqlExecutionInfo{"
            + "overallTook="
            + overallTook
            + ", isPartial="
            + isPartial
            + ", isStopped="
            + isStopped
            + ", clusterInfo="
            + clusterInfo
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EsqlExecutionInfo that = (EsqlExecutionInfo) o;
        return Objects.equals(clusterInfo, that.clusterInfo) && Objects.equals(overallTook, that.overallTook);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterInfo, overallTook);
    }

    public boolean isPartial() {
        return isPartial;
    }

    public void markAsStopped() {
        isStopped = true;
    }

    public boolean isStopped() {
        return isStopped;
    }

    public void clusterInfoInitializing(boolean clusterInfoInitializing) {
        this.clusterInfoInitializing = clusterInfoInitializing;
    }

    public boolean isMainPlan() {
        return inSubplan == false;
    }

    public void startSubPlans() {
        this.inSubplan = true;
    }

    public void finishSubPlans() {
        this.inSubplan = false;
    }

    /**
     * Represents the search metadata about a particular cluster involved in a cross-cluster search.
     * The Cluster object can represent either the local cluster or a remote cluster.
     * For the local cluster, clusterAlias should be specified as RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.
     * Its XContent is put into the "details" section the "_clusters" entry in the REST query response.
     * This is an immutable class, so updates made during the search progress (especially important for async
     * CCS searches) must be done by replacing the Cluster object with a new one.
     */
    public static class Cluster implements ToXContentFragment, Writeable {
        public static final ParseField INDICES_FIELD = new ParseField("indices");
        public static final ParseField STATUS_FIELD = new ParseField("status");
        public static final ParseField TOOK = new ParseField("took");

        private final String clusterAlias;
        private final String indexExpression; // original index expression from the user for this cluster
        private final boolean skipUnavailable;
        private final Cluster.Status status;
        private final Integer totalShards;
        private final Integer successfulShards;
        private final Integer skippedShards;
        private final Integer failedShards;
        private final List<ShardSearchFailure> failures;
        private final TimeValue took;  // search latency for this cluster sub-search

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

        public Cluster(String clusterAlias, String indexExpression) {
            this(clusterAlias, indexExpression, true, Cluster.Status.RUNNING, null, null, null, null, null, null);
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
            this(clusterAlias, indexExpression, skipUnavailable, Cluster.Status.RUNNING, null, null, null, null, null, null);
        }

        /**
         * Create a Cluster with a new Status other than the default of RUNNING.
         * @param clusterAlias clusterAlias as defined in the remote cluster settings or RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY
         *                     for the local cluster
         * @param indexExpression the original (not resolved/concrete) indices expression provided for this cluster.
         * @param skipUnavailable whether cluster is marked as skip_unavailable in remote cluster settings
         * @param status current status of the search on this Cluster
         */
        public Cluster(String clusterAlias, String indexExpression, boolean skipUnavailable, Cluster.Status status) {
            this(clusterAlias, indexExpression, skipUnavailable, status, null, null, null, null, null, null);
        }

        public Cluster(
            String clusterAlias,
            String indexExpression,
            boolean skipUnavailable,
            Cluster.Status status,
            Integer totalShards,
            Integer successfulShards,
            Integer skippedShards,
            Integer failedShards,
            List<ShardSearchFailure> failures,
            TimeValue took
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
            this.failures = failures == null ? Collections.emptyList() : failures;
            this.took = took;
        }

        public Cluster(StreamInput in) throws IOException {
            this.clusterAlias = in.readString();
            this.indexExpression = in.readString();
            this.status = Cluster.Status.valueOf(in.readString().toUpperCase(Locale.ROOT));
            this.totalShards = in.readOptionalInt();
            this.successfulShards = in.readOptionalInt();
            this.skippedShards = in.readOptionalInt();
            this.failedShards = in.readOptionalInt();
            this.took = in.readOptionalTimeValue();
            this.skipUnavailable = in.readBoolean();
            this.failures = Collections.unmodifiableList(in.readCollectionAsList(ShardSearchFailure::readShardSearchFailure));
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
            out.writeOptionalTimeValue(took);
            out.writeBoolean(skipUnavailable);
            out.writeCollection(failures);
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
            private Cluster.Status status;
            private Integer totalShards;
            private Integer successfulShards;
            private Integer skippedShards;
            private Integer failedShards;
            private List<ShardSearchFailure> failures;
            private TimeValue took;
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
                    took != null ? took : original.getTook()
                );
            }

            public Cluster.Builder setStatus(Cluster.Status status) {
                this.status = status;
                return this;
            }

            public Cluster.Builder setTotalShards(int totalShards) {
                this.totalShards = totalShards;
                return this;
            }

            public Cluster.Builder setSuccessfulShards(int successfulShards) {
                this.successfulShards = successfulShards;
                return this;
            }

            public Cluster.Builder setSkippedShards(int skippedShards) {
                this.skippedShards = skippedShards;
                return this;
            }

            public Cluster.Builder setFailedShards(int failedShards) {
                this.failedShards = failedShards;
                return this;
            }

            public Cluster.Builder addFailures(List<ShardSearchFailure> failures) {
                if (failures.isEmpty()) {
                    return this;
                }
                if (this.failures == null) {
                    this.failures = new ArrayList<>(original.failures);
                }
                this.failures.addAll(failures);
                return this;
            }

            public Cluster.Builder setTook(TimeValue took) {
                this.took = took;
                return this;
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String name = clusterAlias;
            if (clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                name = LOCAL_CLUSTER_NAME_REPRESENTATION;
            }
            builder.startObject(name);
            {
                builder.field(STATUS_FIELD.getPreferredName(), getStatus().toString());
                builder.field(INDICES_FIELD.getPreferredName(), indexExpression);
                if (took != null && status != Status.RUNNING) {
                    builder.field(TOOK.getPreferredName(), took.millis());
                }
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

        @Override
        public boolean isFragment() {
            return ToXContentFragment.super.isFragment();
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

        public Cluster.Status getStatus() {
            return status;
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

        public List<ShardSearchFailure> getFailures() {
            return failures;
        }

        boolean isPartial() {
            return status == Status.PARTIAL || status == Status.SKIPPED || (failedShards != null && failedShards > 0);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Cluster cluster = (Cluster) o;
            return Objects.equals(clusterAlias, cluster.clusterAlias)
                && Objects.equals(indexExpression, cluster.indexExpression)
                && status == cluster.status
                && Objects.equals(totalShards, cluster.totalShards)
                && Objects.equals(successfulShards, cluster.successfulShards)
                && Objects.equals(skippedShards, cluster.skippedShards)
                && Objects.equals(failedShards, cluster.failedShards)
                && Objects.equals(took, cluster.took);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterAlias, indexExpression, status, totalShards, successfulShards, skippedShards, failedShards, took);
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
                + ", took="
                + took
                + ", indexExpression='"
                + indexExpression
                + '\''
                + ", skipUnavailable="
                + skipUnavailable
                + '}';
        }
    }
}
