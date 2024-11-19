/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * Represents the state of the cluster, held in memory on all nodes in the cluster with updates coordinated by the elected master.
 * <p>
 * Conceptually immutable, but in practice it has a few components like {@link RoutingNodes} which are pure functions of the immutable state
 * but are expensive to compute so they are built on-demand if needed.
 * <p>
 * The {@link Metadata} portion is written to disk on each update so it persists across full-cluster restarts. The rest of this data is
 * maintained only in-memory and resets back to its initial state on a full-cluster restart, but it is held on all nodes so it persists
 * across master elections (and therefore is preserved in a rolling restart).
 * <p>
 * Updates are triggered by submitting tasks to the {@link MasterService} on the elected master, typically using a {@link
 * TransportMasterNodeAction} to route a request to the master on which the task is submitted via a queue obtained with {@link
 * ClusterService#createTaskQueue}, which has an associated priority. Submitted tasks have an associated
 * timeout. Tasks are processed in priority order, so a flood of higher-priority tasks can starve lower-priority ones from running.
 * Therefore, avoid priorities other than {@link Priority#NORMAL} where possible. Tasks associated with client actions should typically have
 * a timeout, or otherwise be sensitive to client cancellations, to avoid surprises caused by the execution of stale tasks long after they
 * are submitted (since clients themselves tend to time out). In contrast, internal tasks can reasonably have an infinite timeout,
 * especially if a timeout would simply trigger a retry.
 * <p>
 * Tasks that share the same {@link ClusterStateTaskExecutor} instance are processed as a batch. Each batch of tasks yields a new {@link
 * ClusterState} which is published to the cluster by {@link ClusterStatePublisher#publish}. Publication usually works by sending a diff,
 * computed via the {@link Diffable} interface, rather than the full state, although it will fall back to sending the full state if the
 * receiving node is new or it has missed out on an intermediate state for some reason. States and diffs are published using the transport
 * protocol, i.e. the {@link Writeable} interface and friends.
 * <p>
 * When committed, the new state is <i>applied</i> which exposes it to the node via {@link ClusterStateApplier} and {@link
 * ClusterStateListener} callbacks registered with the {@link ClusterApplierService}. The new state is also made available via {@link
 * ClusterService#state()}. The appliers are notified (in no particular order) before {@link ClusterService#state()} is updated, and the
 * listeners are notified (in no particular order) afterwards. Cluster state updates run in sequence, one-by-one, so they can be a
 * performance bottleneck. See the JavaDocs on the linked classes and methods for more details.
 * <p>
 * Cluster state updates can be used to trigger various actions via a {@link ClusterStateListener} rather than using a timer.
 * <p>
 * Implements {@link ChunkedToXContent} to be exposed in REST APIs (e.g. {@code GET _cluster/state} and {@code POST _cluster/reroute}) and
 * to be indexed by monitoring, mostly just for diagnostics purposes. The {@link XContent} representation does not need to be 100% faithful
 * since we never reconstruct a cluster state from its XContent representation, but the more faithful it is the more useful it is for
 * diagnostics. Note that the {@link XContent} representation of the {@link Metadata} portion does have to be faithful (in {@link
 * Metadata.XContentContext#GATEWAY} context) since this is how it persists across full cluster restarts.
 * <p>
 * Security-sensitive data such as passwords or private keys should not be stored in the cluster state, since the contents of the cluster
 * state are exposed in various APIs.
 */
public class ClusterState implements ChunkedToXContent, Diffable<ClusterState> {

    public static final ClusterState EMPTY_STATE = builder(ClusterName.DEFAULT).build();

    public interface Custom extends NamedDiffable<Custom>, ChunkedToXContent {

        /**
         * Returns <code>true</code> iff this {@link Custom} is private to the cluster and should never be sent to a client.
         * The default is <code>false</code>;
         */
        default boolean isPrivate() {
            return false;
        }

        /**
         * Serialize this {@link Custom} for diagnostic purposes, exposed by the <pre>GET _cluster/state</pre> API etc. The XContent
         * representation does not need to be 100% faithful since we never reconstruct a cluster state from its XContent representation, but
         * the more faithful it is the more useful it is for diagnostics.
         */
        @Override
        Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params);
    }

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    private static final DiffableUtils.ValueSerializer<String, CompatibilityVersions> COMPATIBILITY_VERSIONS_VALUE_SERIALIZER =
        new DiffableUtils.NonDiffableValueSerializer<>() {
            @Override
            public void write(CompatibilityVersions value, StreamOutput out) throws IOException {
                value.writeTo(out);
            }

            @Override
            public CompatibilityVersions read(StreamInput in, String key) throws IOException {
                return CompatibilityVersions.readVersion(in);
            }
        };

    public static final String UNKNOWN_UUID = "_na_";

    public static final long UNKNOWN_VERSION = -1;

    /**
     * Monotonically increasing on (and therefore uniquely identifies) <i>committed</i> states. However sometimes a state is created/applied
     * without committing it, for instance to add a {@link NoMasterBlockService#getNoMasterBlock}.
     */
    private final long version;

    /**
     * Uniquely identifies this state, even if the state is not committed.
     */
    private final String stateUUID;

    /**
     * Describes the location (and state) of all shards, used for routing actions such as searches to the relevant shards.
     */
    private final RoutingTable routingTable;

    private final DiscoveryNodes nodes;

    private final Map<String, CompatibilityVersions> compatibilityVersions;
    private final CompatibilityVersions minVersions;

    private final ClusterFeatures clusterFeatures;

    private final Metadata metadata;

    private final ClusterBlocks blocks;

    private final Map<String, Custom> customs;

    private final ClusterName clusterName;

    private final boolean wasReadFromDiff;

    // built on demand
    private volatile RoutingNodes routingNodes;

    public ClusterState(long version, String stateUUID, ClusterState state) {
        this(
            state.clusterName,
            version,
            stateUUID,
            state.metadata(),
            state.routingTable(),
            state.nodes(),
            state.compatibilityVersions,
            state.clusterFeatures(),
            state.blocks(),
            state.customs(),
            false,
            state.routingNodes
        );
    }

    public ClusterState(
        ClusterName clusterName,
        long version,
        String stateUUID,
        Metadata metadata,
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        Map<String, CompatibilityVersions> compatibilityVersions,
        ClusterFeatures clusterFeatures,
        ClusterBlocks blocks,
        Map<String, Custom> customs,
        boolean wasReadFromDiff,
        @Nullable RoutingNodes routingNodes
    ) {
        this.version = version;
        this.stateUUID = stateUUID;
        this.clusterName = clusterName;
        this.metadata = metadata;
        this.routingTable = routingTable;
        this.nodes = nodes;
        this.compatibilityVersions = Map.copyOf(compatibilityVersions);
        this.clusterFeatures = clusterFeatures;
        this.blocks = blocks;
        this.customs = customs;
        this.wasReadFromDiff = wasReadFromDiff;
        this.routingNodes = routingNodes;
        assert assertConsistentRoutingNodes(routingTable, nodes, routingNodes);
        this.minVersions = blocks.hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
            ? new CompatibilityVersions(TransportVersions.MINIMUM_COMPATIBLE, Map.of()) // empty map because cluster state is unknown
            : CompatibilityVersions.minimumVersions(compatibilityVersions.values());

        assert compatibilityVersions.isEmpty()
            || blocks.hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
            || assertEventIngestedIsUnknownInMixedClusters(metadata, this.minVersions);
    }

    private boolean assertEventIngestedIsUnknownInMixedClusters(Metadata metadata, CompatibilityVersions compatibilityVersions) {
        if (compatibilityVersions.transportVersion().before(TransportVersions.V_8_15_0) && metadata != null && metadata.indices() != null) {
            for (IndexMetadata indexMetadata : metadata.indices().values()) {
                assert indexMetadata.getEventIngestedRange() == IndexLongFieldRange.UNKNOWN
                    : "event.ingested range should be UNKNOWN but is "
                        + indexMetadata.getEventIngestedRange()
                        + " for index: "
                        + indexMetadata.getIndex()
                        + " minTransportVersion: "
                        + compatibilityVersions.transportVersion();
            }
        }
        return true;
    }

    private static boolean assertConsistentRoutingNodes(
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        @Nullable RoutingNodes routingNodes
    ) {
        if (routingNodes == null) {
            return true;
        }
        final RoutingNodes expected = RoutingNodes.immutable(routingTable, nodes);
        assert routingNodes.equals(expected)
            : "RoutingNodes [" + routingNodes + "] are not consistent with this cluster state [" + expected + "]";
        return true;
    }

    public long term() {
        return coordinationMetadata().term();
    }

    public long version() {
        return this.version;
    }

    public long getVersion() {
        return version();
    }

    /**
     * This stateUUID is automatically generated for for each version of cluster state. It is used to make sure that
     * we are applying diffs to the right previous state.
     */
    public String stateUUID() {
        return this.stateUUID;
    }

    public DiscoveryNodes nodes() {
        return this.nodes;
    }

    public DiscoveryNodes getNodes() {
        return nodes();
    }

    /**
     * Returns the set of nodes that should be exposed to things like REST handlers that behave differently depending on the nodes in the
     * cluster and their versions. Specifically, if the cluster has properly formed then this is the nodes in the last-applied cluster
     * state, but if the cluster has not properly formed then no nodes are returned.
     *
     * @return the nodes in the cluster if the cluster has properly formed, otherwise an empty set of nodes.
     */
    public DiscoveryNodes nodesIfRecovered() {
        return blocks.hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) ? DiscoveryNodes.EMPTY_NODES : nodes;
    }

    public boolean clusterRecovered() {
        return blocks.hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
    }

    public Map<String, CompatibilityVersions> compatibilityVersions() {
        return this.compatibilityVersions;
    }

    public boolean hasMixedSystemIndexVersions() {
        return compatibilityVersions.values()
            .stream()
            .anyMatch(e -> e.systemIndexMappingsVersion().equals(minVersions.systemIndexMappingsVersion()) == false);
    }

    public TransportVersion getMinTransportVersion() {
        return this.minVersions.transportVersion();
    }

    public Map<String, SystemIndexDescriptor.MappingsVersion> getMinSystemIndexMappingVersions() {
        return this.minVersions.systemIndexMappingsVersion();
    }

    public ClusterFeatures clusterFeatures() {
        return clusterFeatures;
    }

    public Metadata metadata() {
        return this.metadata;
    }

    public Metadata getMetadata() {
        return metadata();
    }

    public CoordinationMetadata coordinationMetadata() {
        return metadata.coordinationMetadata();
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ClusterBlocks getBlocks() {
        return blocks;
    }

    public Map<String, Custom> customs() {
        return this.customs;
    }

    public Map<String, Custom> getCustoms() {
        return this.customs;
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return coordinationMetadata().getLastAcceptedConfiguration();
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return coordinationMetadata().getLastCommittedConfiguration();
    }

    public Set<VotingConfigExclusion> getVotingConfigExclusions() {
        return coordinationMetadata().getVotingConfigExclusions();
    }

    /**
     * Returns a built (on demand) routing nodes view of the routing table.
     */
    public RoutingNodes getRoutingNodes() {
        RoutingNodes r = routingNodes;
        if (r != null) {
            return r;
        }
        r = buildRoutingNodes();
        return r;
    }

    private synchronized RoutingNodes buildRoutingNodes() {
        RoutingNodes r = routingNodes;
        if (r != null) {
            return r;
        }
        r = RoutingNodes.immutable(routingTable, nodes);
        routingNodes = r;
        return r;
    }

    /**
     * Returns a fresh mutable copy of the routing nodes view.
     */
    public RoutingNodes mutableRoutingNodes() {
        final RoutingNodes nodes = this.routingNodes;
        // use the cheaper copy constructor if we already computed the routing nodes for this state.
        if (nodes != null) {
            return nodes.mutableCopy();
        }
        // we don't have any routing nodes for this state, likely because it's a temporary state in the reroute logic, don't compute an
        // immutable copy that will never be used and instead directly build a mutable copy
        return RoutingNodes.mutable(routingTable, this.nodes);
    }

    /**
     * Initialize data structures that lazy computed for this instance in the background by using the giving executor.
     * @param executor executor to run initialization tasks on
     */
    public void initializeAsync(Executor executor) {
        if (routingNodes == null) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    getRoutingNodes();
                }

                @Override
                public String toString() {
                    return "async initialization of routing nodes for cluster state " + version();
                }
            });
        }
        if (metadata.indicesLookupInitialized() == false) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    metadata.getIndicesLookup();
                }

                @Override
                public String toString() {
                    return "async initialization of indices lookup for cluster state " + version();
                }
            });
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        final String TAB = "   ";
        sb.append("cluster uuid: ")
            .append(metadata.clusterUUID())
            .append(" [committed: ")
            .append(metadata.clusterUUIDCommitted())
            .append("]")
            .append("\n");
        sb.append("version: ").append(version).append("\n");
        sb.append("state uuid: ").append(stateUUID).append("\n");
        sb.append("from_diff: ").append(wasReadFromDiff).append("\n");
        sb.append("meta data version: ").append(metadata.version()).append("\n");
        sb.append(TAB).append("coordination_metadata:\n");
        sb.append(TAB).append(TAB).append("term: ").append(coordinationMetadata().term()).append("\n");
        sb.append(TAB)
            .append(TAB)
            .append("last_committed_config: ")
            .append(coordinationMetadata().getLastCommittedConfiguration())
            .append("\n");
        sb.append(TAB)
            .append(TAB)
            .append("last_accepted_config: ")
            .append(coordinationMetadata().getLastAcceptedConfiguration())
            .append("\n");
        sb.append(TAB).append(TAB).append("voting tombstones: ").append(coordinationMetadata().getVotingConfigExclusions()).append("\n");
        for (IndexMetadata indexMetadata : metadata) {
            sb.append(TAB).append(indexMetadata.getIndex());
            sb.append(": v[")
                .append(indexMetadata.getVersion())
                .append("], mv[")
                .append(indexMetadata.getMappingVersion())
                .append("], sv[")
                .append(indexMetadata.getSettingsVersion())
                .append("], av[")
                .append(indexMetadata.getAliasesVersion())
                .append("]\n");
            for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                sb.append(TAB).append(TAB).append(shard).append(": ");
                sb.append("p_term [").append(indexMetadata.primaryTerm(shard)).append("], ");
                sb.append("isa_ids ").append(indexMetadata.inSyncAllocationIds(shard)).append("\n");
            }
        }
        if (metadata.customs().isEmpty() == false) {
            sb.append("metadata customs:\n");
            for (final Map.Entry<String, Metadata.Custom> cursor : metadata.customs().entrySet()) {
                final String type = cursor.getKey();
                final Metadata.Custom custom = cursor.getValue();
                sb.append(TAB).append(type).append(": ").append(custom);
            }
            sb.append("\n");
        }
        sb.append(blocks());
        sb.append(nodes());
        if (compatibilityVersions.isEmpty() == false) {
            sb.append("node versions:\n");
            for (var tv : compatibilityVersions.entrySet()) {
                sb.append(TAB).append(tv.getKey()).append(": ").append(tv.getValue()).append("\n");
            }
        }
        sb.append("cluster features:\n");
        for (var nf : getNodeFeatures(clusterFeatures).entrySet()) {
            sb.append(TAB).append(nf.getKey()).append(": ").append(new TreeSet<>(nf.getValue())).append("\n");
        }
        sb.append(routingTable());
        sb.append(getRoutingNodes());
        if (customs.isEmpty() == false) {
            sb.append("customs:\n");
            for (Map.Entry<String, Custom> cursor : customs.entrySet()) {
                final String type = cursor.getKey();
                final Custom custom = cursor.getValue();
                sb.append(TAB).append(type).append(": ").append(custom);
            }
        }
        return sb.toString();
    }

    /**
     * a cluster state supersedes another state if they are from the same master and the version of this state is higher than that of the
     * other state.
     * <p>
     * In essence that means that all the changes from the other cluster state are also reflected by the current one
     */
    public boolean supersedes(ClusterState other) {
        return this.nodes().getMasterNodeId() != null
            && this.nodes().getMasterNodeId().equals(other.nodes().getMasterNodeId())
            && this.version() > other.version();

    }

    public enum Metric {
        VERSION("version"),
        MASTER_NODE("master_node"),
        BLOCKS("blocks"),
        NODES("nodes"),
        METADATA("metadata"),
        ROUTING_TABLE("routing_table"),
        ROUTING_NODES("routing_nodes"),
        CUSTOMS("customs");

        private static final Map<String, Metric> valueToEnum;

        static {
            valueToEnum = new HashMap<>();
            for (Metric metric : Metric.values()) {
                valueToEnum.put(metric.value, metric);
            }
        }

        private final String value;

        Metric(String value) {
            this.value = value;
        }

        public static EnumSet<Metric> parseString(String param, boolean ignoreUnknown) {
            String[] metrics = Strings.splitStringByCommaToArray(param);
            EnumSet<Metric> result = EnumSet.noneOf(Metric.class);
            for (String metric : metrics) {
                if ("_all".equals(metric)) {
                    result = EnumSet.allOf(Metric.class);
                    break;
                }
                Metric m = valueToEnum.get(metric);
                if (m == null) {
                    if (ignoreUnknown == false) {
                        throw new IllegalArgumentException("Unknown metric [" + metric + "]");
                    }
                } else {
                    result.add(m);
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private static <T> Iterator<ToXContent> chunkedSection(
        boolean condition,
        ToXContent before,
        Iterator<T> items,
        Function<T, Iterator<ToXContent>> fn,
        ToXContent after
    ) {
        return condition
            ? Iterators.concat(Iterators.single(before), Iterators.flatMap(items, fn::apply), Iterators.single(after))
            : Collections.emptyIterator();
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        final var metrics = Metric.parseString(outerParams.param("metric", "_all"), true);

        return Iterators.concat(

            // header chunk
            Iterators.single(((builder, params) -> {
                // always provide the cluster_uuid as part of the top-level response (also part of the metadata response)
                builder.field("cluster_uuid", metadata().clusterUUID());

                // state version info
                if (metrics.contains(Metric.VERSION)) {
                    builder.field("version", version);
                    builder.field("state_uuid", stateUUID);
                }

                // master node
                if (metrics.contains(Metric.MASTER_NODE)) {
                    builder.field("master_node", nodes().getMasterNodeId());
                }

                return builder;
            })),

            // blocks
            chunkedSection(metrics.contains(Metric.BLOCKS), (builder, params) -> {
                builder.startObject("blocks");
                if (blocks().global().isEmpty() == false) {
                    builder.startObject("global");
                    for (ClusterBlock block : blocks().global()) {
                        block.toXContent(builder, params);
                    }
                    builder.endObject();
                }
                if (blocks().indices().isEmpty() == false) {
                    builder.startObject("indices");
                }
                return builder;
            }, blocks.indices().entrySet().iterator(), entry -> Iterators.single((builder, params) -> {
                builder.startObject(entry.getKey());
                for (ClusterBlock block : entry.getValue()) {
                    block.toXContent(builder, params);
                }
                return builder.endObject();
            }), (builder, params) -> {
                if (blocks().indices().isEmpty() == false) {
                    builder.endObject();
                }
                return builder.endObject();
            }),

            // nodes
            chunkedSection(
                metrics.contains(Metric.NODES),
                (builder, params) -> builder.startObject("nodes"),
                nodes.iterator(),
                Iterators::single,
                (builder, params) -> builder.endObject()
            ),

            // per-node version information
            chunkedSection(
                metrics.contains(Metric.NODES),
                (builder, params) -> builder.startArray("nodes_versions"),
                compatibilityVersions.entrySet().iterator(),
                e -> Iterators.single((builder, params) -> {
                    builder.startObject().field("node_id", e.getKey());
                    e.getValue().toXContent(builder, params);
                    return builder.endObject();
                }),
                (builder, params) -> builder.endArray()
            ),

            // per-node feature information
            metrics.contains(Metric.NODES)
                ? Iterators.concat(
                    Iterators.<ToXContent>single((b, p) -> b.field("nodes_features")),
                    clusterFeatures.toXContentChunked(outerParams)
                )
                : Collections.emptyIterator(),

            // metadata
            metrics.contains(Metric.METADATA) ? metadata.toXContentChunked(outerParams) : Collections.emptyIterator(),

            // routing table
            chunkedSection(
                metrics.contains(Metric.ROUTING_TABLE),
                (builder, params) -> builder.startObject("routing_table").startObject("indices"),
                routingTable().iterator(),
                indexRoutingTable -> {
                    Iterator<Iterator<ToXContent>> input = Iterators.forRange(0, indexRoutingTable.size(), shardId -> {
                        final var indexShardRoutingTable = indexRoutingTable.shard(shardId);
                        return Iterators.concat(
                            Iterators.single(
                                (builder, params) -> builder.startArray(Integer.toString(indexShardRoutingTable.shardId().id()))
                            ),
                            Iterators.forRange(
                                0,
                                indexShardRoutingTable.size(),
                                copy -> (builder, params) -> indexShardRoutingTable.shard(copy).toXContent(builder, params)
                            ),
                            Iterators.single((builder, params) -> builder.endArray())
                        );
                    });
                    return Iterators.concat(
                        Iterators.single(
                            (builder, params) -> builder.startObject(indexRoutingTable.getIndex().getName()).startObject("shards")
                        ),
                        Iterators.flatMap(input, Function.identity()),
                        Iterators.single((builder, params) -> builder.endObject().endObject())
                    );
                },
                (builder, params) -> builder.endObject().endObject()
            ),

            // routing nodes
            chunkedSection(
                metrics.contains(Metric.ROUTING_NODES),
                (builder, params) -> builder.startObject("routing_nodes").startArray("unassigned"),
                getRoutingNodes().unassigned().iterator(),
                Iterators::single,
                (builder, params) -> builder.endArray() // no endObject() here, continued in next chunkedSection()
            ),
            chunkedSection(
                metrics.contains(Metric.ROUTING_NODES),
                (builder, params) -> builder.startObject("nodes"),
                getRoutingNodes().iterator(),
                routingNode -> Iterators.concat(
                    ChunkedToXContentHelper.startArray(routingNode.nodeId() == null ? "null" : routingNode.nodeId()),
                    routingNode.iterator(),
                    ChunkedToXContentHelper.endArray()
                ),
                (builder, params) -> builder.endObject().endObject()
            ),

            // customs
            metrics.contains(Metric.CUSTOMS)
                ? ChunkedToXContent.builder(outerParams)
                    .forEach(customs.entrySet().iterator(), (b, e) -> b.xContentObject(e.getKey(), e.getValue()))
                : Collections.emptyIterator()
        );
    }

    public static Builder builder(ClusterName clusterName) {
        return new Builder(clusterName);
    }

    public static Builder builder(ClusterState state) {
        return new Builder(state);
    }

    public ClusterState copyAndUpdate(Consumer<Builder> updater) {
        var builder = builder(this);
        updater.accept(builder);
        return builder.build();
    }

    public ClusterState copyAndUpdateMetadata(Consumer<Metadata.Builder> updater) {
        return copyAndUpdate(builder -> builder.metadata(metadata().copyAndUpdate(updater)));
    }

    @SuppressForbidden(reason = "directly reading ClusterState#clusterFeatures")
    private static Map<String, Set<String>> getNodeFeatures(ClusterFeatures features) {
        return features.nodeFeatures();
    }

    public static class Builder {

        private ClusterState previous;

        private final ClusterName clusterName;
        private long version = 0;
        private String uuid = UNKNOWN_UUID;
        private Metadata metadata = Metadata.EMPTY_METADATA;
        private RoutingTable routingTable = RoutingTable.EMPTY_ROUTING_TABLE;
        private DiscoveryNodes nodes = DiscoveryNodes.EMPTY_NODES;
        private final Map<String, CompatibilityVersions> compatibilityVersions;
        private final Map<String, Set<String>> nodeFeatures;
        private ClusterBlocks blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
        private final ImmutableOpenMap.Builder<String, Custom> customs;
        private boolean fromDiff;

        public Builder(ClusterState state) {
            this.previous = state;
            this.clusterName = state.clusterName;
            this.version = state.version();
            this.uuid = state.stateUUID();
            this.nodes = state.nodes();
            this.compatibilityVersions = new HashMap<>(state.compatibilityVersions);
            this.nodeFeatures = new HashMap<>(getNodeFeatures(state.clusterFeatures()));
            this.routingTable = state.routingTable();
            this.metadata = state.metadata();
            this.blocks = state.blocks();
            this.customs = ImmutableOpenMap.builder(state.customs());
            this.fromDiff = false;
        }

        public Builder(ClusterName clusterName) {
            this.compatibilityVersions = new HashMap<>();
            this.nodeFeatures = new HashMap<>();
            customs = ImmutableOpenMap.builder();
            this.clusterName = clusterName;
        }

        public Builder nodes(DiscoveryNodes.Builder nodesBuilder) {
            return nodes(nodesBuilder.build());
        }

        public Builder nodes(DiscoveryNodes nodes) {
            this.nodes = nodes;
            return this;
        }

        public DiscoveryNodes nodes() {
            return nodes;
        }

        public Builder putCompatibilityVersions(
            String nodeId,
            TransportVersion transportVersion,
            Map<String, SystemIndexDescriptor.MappingsVersion> systemIndexMappingsVersions
        ) {
            return putCompatibilityVersions(
                nodeId,
                new CompatibilityVersions(Objects.requireNonNull(transportVersion, nodeId), systemIndexMappingsVersions)
            );
        }

        public Builder putCompatibilityVersions(String nodeId, CompatibilityVersions versions) {
            compatibilityVersions.put(nodeId, versions);
            return this;
        }

        public Builder nodeIdsToCompatibilityVersions(Map<String, CompatibilityVersions> versions) {
            versions.forEach((key, value) -> Objects.requireNonNull(value, key));
            // remove all versions not present in the new map
            this.compatibilityVersions.keySet().retainAll(versions.keySet());
            this.compatibilityVersions.putAll(versions);
            return this;
        }

        public Map<String, CompatibilityVersions> compatibilityVersions() {
            return Collections.unmodifiableMap(this.compatibilityVersions);
        }

        public Builder nodeFeatures(ClusterFeatures features) {
            this.nodeFeatures.clear();
            this.nodeFeatures.putAll(getNodeFeatures(features));
            return this;
        }

        public Builder nodeFeatures(Map<String, Set<String>> nodeFeatures) {
            this.nodeFeatures.clear();
            this.nodeFeatures.putAll(nodeFeatures);
            return this;
        }

        public Map<String, Set<String>> nodeFeatures() {
            return Collections.unmodifiableMap(this.nodeFeatures);
        }

        public Builder putNodeFeatures(String node, Set<String> features) {
            this.nodeFeatures.put(node, features);
            return this;
        }

        public Builder routingTable(RoutingTable.Builder routingTableBuilder) {
            return routingTable(routingTableBuilder.build());
        }

        public Builder routingTable(RoutingTable routingTable) {
            this.routingTable = routingTable;
            return this;
        }

        public Builder metadata(Metadata.Builder metadataBuilder) {
            return metadata(metadataBuilder.build());
        }

        public Builder metadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder blocks(ClusterBlocks.Builder blocksBuilder) {
            return blocks(blocksBuilder.build());
        }

        public Builder blocks(ClusterBlocks blocks) {
            this.blocks = blocks;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder incrementVersion() {
            this.version = version + 1;
            this.uuid = UNKNOWN_UUID;
            return this;
        }

        public Builder stateUUID(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(Map<String, Custom> customs) {
            customs.forEach((key, value) -> Objects.requireNonNull(value, key));
            this.customs.putAllFromMap(customs);
            return this;
        }

        // set previous cluster state that this builder is created from during diff application
        private Builder fromDiff(ClusterState previous) {
            this.fromDiff = true;
            this.previous = previous;
            return this;
        }

        public ClusterState build() {
            if (UNKNOWN_UUID.equals(uuid)) {
                uuid = UUIDs.randomBase64UUID();
            }
            final RoutingNodes routingNodes;
            if (previous != null && routingTable.indicesRouting() == previous.routingTable.indicesRouting() && nodes == previous.nodes) {
                // routing table contents and nodes haven't changed so we can try to reuse the previous state's routing nodes which are
                // expensive to compute
                routingNodes = previous.routingNodes;
            } else {
                routingNodes = null;
            }

            // ensure every node in the cluster has a feature set
            // nodes can be null in some tests
            if (nodes != null) {
                for (DiscoveryNode node : nodes) {
                    nodeFeatures.putIfAbsent(node.getId(), Set.of());
                }
            }

            return new ClusterState(
                clusterName,
                version,
                uuid,
                metadata,
                routingTable,
                nodes,
                compatibilityVersions,
                previous != null && getNodeFeatures(previous.clusterFeatures).equals(nodeFeatures)
                    ? previous.clusterFeatures
                    : new ClusterFeatures(nodeFeatures),
                blocks,
                customs.build(),
                fromDiff,
                routingNodes
            );
        }

        public static byte[] toBytes(ClusterState state) throws IOException {
            BytesStreamOutput os = new BytesStreamOutput();
            state.writeTo(os);
            return BytesReference.toBytes(os.bytes());
        }

        /**
         * @param data      input bytes
         * @param localNode used to set the local node in the cluster state.
         */
        public static ClusterState fromBytes(byte[] data, DiscoveryNode localNode, NamedWriteableRegistry registry) throws IOException {
            StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(data), registry);
            return readFrom(in, localNode);

        }
    }

    @Override
    public Diff<ClusterState> diff(ClusterState previousState) {
        return new ClusterStateDiff(previousState, this);
    }

    public static Diff<ClusterState> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return new ClusterStateDiff(in, localNode);
    }

    public static ClusterState readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        ClusterName clusterName = new ClusterName(in);
        Builder builder = new Builder(clusterName);
        builder.version = in.readLong();
        builder.uuid = in.readString();
        builder.metadata = Metadata.readFrom(in);
        builder.routingTable = RoutingTable.readFrom(in);
        builder.nodes = DiscoveryNodes.readFrom(in, localNode);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            builder.nodeIdsToCompatibilityVersions(in.readMap(CompatibilityVersions::readVersion));
        } else {
            // this clusterstate is from a pre-8.8.0 node
            // infer the versions from discoverynodes for now
            // leave mappings versions empty
            builder.nodes()
                .getNodes()
                .values()
                .forEach(n -> builder.putCompatibilityVersions(n.getId(), inferTransportVersion(n), Map.of()));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            builder.nodeFeatures(ClusterFeatures.readFrom(in));
        }
        builder.blocks = ClusterBlocks.readFrom(in);
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readVInt(); // used to be minimumMasterNodesOnPublishingMaster, which was used in 7.x for BWC with 6.x
        }
        return builder.build();
    }

    /**
     * If the cluster state does not contain transport version information, this is the version
     * that is inferred for all nodes on version 8.8.0 or above.
     */
    public static final TransportVersion INFERRED_TRANSPORT_VERSION = TransportVersions.V_8_8_0;

    public static final Version VERSION_INTRODUCING_TRANSPORT_VERSIONS = Version.V_8_8_0;

    private static TransportVersion inferTransportVersion(DiscoveryNode node) {
        TransportVersion tv;
        if (node.getVersion().before(VERSION_INTRODUCING_TRANSPORT_VERSIONS)) {
            // 1-to-1 mapping between Version and TransportVersion
            tv = TransportVersion.fromId(node.getPre811VersionId().getAsInt());
        } else {
            // use the lowest value it could be for now
            tv = INFERRED_TRANSPORT_VERSION;
        }
        return tv;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeLong(version);
        out.writeString(stateUUID);
        metadata.writeTo(out);
        routingTable.writeTo(out);
        nodes.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeMap(compatibilityVersions, StreamOutput::writeWriteable);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            clusterFeatures.writeTo(out);
        }
        blocks.writeTo(out);
        VersionedNamedWriteable.writeVersionedWritables(out, customs);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeVInt(-1); // used to be minimumMasterNodesOnPublishingMaster, which was used in 7.x for BWC with 6.x
        }
    }

    private static class ClusterStateDiff implements Diff<ClusterState> {

        private final long toVersion;

        private final String fromUuid;

        private final String toUuid;

        private final ClusterName clusterName;

        private final Diff<RoutingTable> routingTable;

        private final Diff<DiscoveryNodes> nodes;

        @Nullable
        private final Diff<Map<String, CompatibilityVersions>> versions;
        private final Diff<ClusterFeatures> features;

        private final Diff<Metadata> metadata;

        private final Diff<ClusterBlocks> blocks;

        private final Diff<Map<String, Custom>> customs;

        ClusterStateDiff(ClusterState before, ClusterState after) {
            fromUuid = before.stateUUID;
            toUuid = after.stateUUID;
            toVersion = after.version;
            clusterName = after.clusterName;
            routingTable = after.routingTable.diff(before.routingTable);
            nodes = after.nodes.diff(before.nodes);
            versions = DiffableUtils.diff(
                before.compatibilityVersions,
                after.compatibilityVersions,
                DiffableUtils.getStringKeySerializer(),
                COMPATIBILITY_VERSIONS_VALUE_SERIALIZER
            );
            features = after.clusterFeatures.diff(before.clusterFeatures);
            metadata = after.metadata.diff(before.metadata);
            blocks = after.blocks.diff(before.blocks);
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        ClusterStateDiff(StreamInput in, DiscoveryNode localNode) throws IOException {
            clusterName = new ClusterName(in);
            fromUuid = in.readString();
            toUuid = in.readString();
            toVersion = in.readLong();
            routingTable = RoutingTable.readDiffFrom(in);
            nodes = DiscoveryNodes.readDiffFrom(in, localNode);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0) && in.readBoolean()) {
                versions = DiffableUtils.readJdkMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    COMPATIBILITY_VERSIONS_VALUE_SERIALIZER
                );
            } else {
                versions = null;   // infer at application time
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                features = ClusterFeatures.readDiffFrom(in);
            } else {
                features = null;    // fill in when nodes re-register with a master that understands features
            }
            metadata = Metadata.readDiffFrom(in);
            blocks = ClusterBlocks.readDiffFrom(in);
            customs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                in.readVInt(); // used to be minimumMasterNodesOnPublishingMaster, which was used in 7.x for BWC with 6.x
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterName.writeTo(out);
            out.writeString(fromUuid);
            out.writeString(toUuid);
            out.writeLong(toVersion);
            routingTable.writeTo(out);
            nodes.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                out.writeOptionalWriteable(versions);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                features.writeTo(out);
            }
            metadata.writeTo(out);
            blocks.writeTo(out);
            customs.writeTo(out);
            if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                out.writeVInt(-1); // used to be minimumMasterNodesOnPublishingMaster, which was used in 7.x for BWC with 6.x
            }
        }

        @Override
        public ClusterState apply(ClusterState state) {
            Builder builder = new Builder(clusterName);
            if (toUuid.equals(state.stateUUID)) {
                // no need to read the rest - cluster state didn't change
                return state;
            }
            if (fromUuid.equals(state.stateUUID) == false) {
                throw new IncompatibleClusterStateVersionException(state.version, state.stateUUID, toVersion, fromUuid);
            }
            builder.stateUUID(toUuid);
            builder.version(toVersion);
            builder.routingTable(routingTable.apply(state.routingTable));
            builder.nodes(nodes.apply(state.nodes));
            if (versions != null) {
                builder.nodeIdsToCompatibilityVersions(this.versions.apply(state.compatibilityVersions));
            } else {
                // infer the versions from discoverynodes for now
                // leave mappings versions empty
                builder.nodes()
                    .getNodes()
                    .values()
                    .forEach(n -> builder.putCompatibilityVersions(n.getId(), inferTransportVersion(n), Map.of()));
            }
            if (features != null) {
                builder.nodeFeatures(this.features.apply(state.clusterFeatures));
            }
            builder.metadata(metadata.apply(state.metadata));
            builder.blocks(blocks.apply(state.blocks));
            builder.customs(customs.apply(state.customs));
            builder.fromDiff(state);
            return builder.build();
        }
    }
}
