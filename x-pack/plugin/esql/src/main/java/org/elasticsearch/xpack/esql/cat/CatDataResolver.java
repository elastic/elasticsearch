/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoMetrics;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fetches data from CAT transport actions and builds {@link Table} objects for each requested
 * endpoint. Used during the pre-analysis phase of ES|QL query planning so that the data is
 * available at analysis time when {@link org.elasticsearch.xpack.esql.analysis.Analyzer} converts
 * {@link org.elasticsearch.xpack.esql.plan.logical.UnresolvedCatRelation} nodes into
 * {@link org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation} nodes.
 *
 * <p>All transport actions are executed under the calling user's security context — no privilege
 * escalation occurs.
 */
public class CatDataResolver {

    private final Client client;
    private final ProjectResolver projectResolver;

    public CatDataResolver(Client client, ProjectResolver projectResolver) {
        this.client = client;
        this.projectResolver = projectResolver;
    }

    /**
     * Resolves each endpoint in {@code endpoints} in parallel, then calls {@code listener} with
     * the collected {@link CatDataResolution}.
     */
    public void resolve(List<String> endpoints, ActionListener<CatDataResolution> listener) {
        Map<String, Table> tables = new ConcurrentHashMap<>();
        try (var listeners = new RefCountingListener(listener.map(ignored -> new CatDataResolution(tables)))) {
            for (String endpoint : endpoints) {
                resolveEndpoint(endpoint, listeners.acquire(table -> tables.put(endpoint, table)));
            }
        }
    }

    private void resolveEndpoint(String endpoint, ActionListener<Table> listener) {
        switch (endpoint) {
            case "health" -> resolveHealth(listener);
            case "aliases" -> resolveAliases(listener);
            case "shards" -> resolveShards(listener);
            case "indices" -> resolveIndices(listener);
            case "nodes" -> resolveNodes(listener);
            case "allocation" -> resolveAllocation(listener);
            default -> listener.onFailure(
                new IllegalArgumentException(
                    "Unknown CAT endpoint [" + endpoint + "]. Available: health, indices, nodes, shards, aliases, allocation"
                )
            );
        }
    }

    // ---- health --------------------------------------------------------------------------------

    private void resolveHealth(ActionListener<Table> listener) {
        client.admin().cluster().health(new ClusterHealthRequest(TimeValue.THIRTY_SECONDS), listener.map(this::buildHealthTable));
    }

    private Table buildHealthTable(ClusterHealthResponse health) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("cluster");
        t.addCell("status");
        t.addCell("node.total");
        t.addCell("node.data");
        t.addCell("shards");
        t.addCell("pri");
        t.addCell("relo");
        t.addCell("init");
        t.addCell("unassign");
        t.addCell("unassign.pri");
        t.addCell("pending_tasks");
        t.addCell("max_task_wait_time");
        t.addCell("active_shards_percent");
        t.endHeaders();

        t.startRow();
        t.addCell(health.getClusterName());
        t.addCell(health.getStatus().name().toLowerCase(Locale.ROOT));
        t.addCell(health.getNumberOfNodes());
        t.addCell(health.getNumberOfDataNodes());
        t.addCell(health.getActiveShards());
        t.addCell(health.getActivePrimaryShards());
        t.addCell(health.getRelocatingShards());
        t.addCell(health.getInitializingShards());
        t.addCell(health.getUnassignedShards());
        t.addCell(health.getUnassignedPrimaryShards());
        t.addCell(health.getNumberOfPendingTasks());
        // max_task_wait_time: store as Long millis; 0 means "no tasks pending" → null
        long waitMillis = health.getTaskMaxWaitingTime().millis();
        t.addCell(waitMillis == 0 ? null : waitMillis);
        t.addCell(String.format(Locale.ROOT, "%1.1f%%", health.getActiveShardsPercent()));
        t.endRow();

        return t;
    }

    // ---- aliases -------------------------------------------------------------------------------

    private void resolveAliases(ActionListener<Table> listener) {
        GetAliasesRequest request = new GetAliasesRequest(TimeValue.timeValueSeconds(30));
        client.admin().indices().getAliases(request, listener.map(this::buildAliasesTable));
    }

    private Table buildAliasesTable(GetAliasesResponse response) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("alias");
        t.addCell("index");
        t.addCell("filter");
        t.addCell("routing.index");
        t.addCell("routing.search");
        t.addCell("is_write_index");
        t.endHeaders();

        for (Map.Entry<String, List<AliasMetadata>> entry : response.getAliases().entrySet()) {
            String indexName = entry.getKey();
            for (AliasMetadata aliasMetadata : entry.getValue()) {
                t.startRow();
                t.addCell(aliasMetadata.alias());
                t.addCell(indexName);
                t.addCell(aliasMetadata.filteringRequired() ? "*" : "-");
                t.addCell(Strings.hasLength(aliasMetadata.indexRouting()) ? aliasMetadata.indexRouting() : "-");
                t.addCell(Strings.hasLength(aliasMetadata.searchRouting()) ? aliasMetadata.searchRouting() : "-");
                t.addCell(aliasMetadata.writeIndex() == null ? "-" : aliasMetadata.writeIndex().toString());
                t.endRow();
            }
        }

        return t;
    }

    // ---- shards --------------------------------------------------------------------------------

    private void resolveShards(ActionListener<Table> listener) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest(TimeValue.timeValueSeconds(30)).clear()
            .nodes(true)
            .routingTable(true);

        ListenableFuture<ClusterStateResponse> clusterStateFuture = new ListenableFuture<>();
        client.admin().cluster().state(clusterStateRequest, clusterStateFuture);

        client.admin()
            .indices()
            .stats(
                new IndicesStatsRequest().all().indicesOptions(IndicesOptions.strictExpandHidden()),
                listener.delegateFailure(
                    (delegate, indicesStatsResponse) -> clusterStateFuture.addListener(
                        delegate.map(clusterStateResponse -> buildShardsTable(clusterStateResponse, indicesStatsResponse))
                    )
                )
            );
    }

    private Table buildShardsTable(ClusterStateResponse clusterStateResponse, IndicesStatsResponse indicesStatsResponse) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("index");
        t.addCell("shard");
        t.addCell("prirep");
        t.addCell("state");
        t.addCell("docs");
        t.addCell("store");
        t.addCell("dataset");
        t.addCell("ip");
        t.addCell("node");
        t.endHeaders();

        Map<ShardRouting, ShardStats> shardStatsMap = indicesStatsResponse.asMap();

        for (ShardRouting shard : clusterStateResponse.getState().routingTable().allShardsIterator()) {
            ShardStats shardStats = shardStatsMap.get(shard);
            CommonStats commonStats = shardStats != null ? shardStats.getStats() : null;

            t.startRow();
            t.addCell(shard.getIndexName());
            t.addCell(shard.id());
            t.addCell(shard.primary() ? "p" : "r");
            t.addCell(shard.state().toString());
            t.addCell(getStatLong(commonStats, s -> s.getDocs() != null ? s.getDocs().getCount() : null));
            t.addCell(getStatLong(commonStats, s -> s.getStore() != null ? s.getStore().size().getBytes() : null));
            t.addCell(getStatLong(commonStats, s -> s.getStore() != null ? s.getStore().totalDataSetSize().getBytes() : null));
            if (shard.assignedToNode()) {
                DiscoveryNode node = clusterStateResponse.getState().nodes().get(shard.currentNodeId());
                t.addCell(node != null ? node.getHostAddress() : null);
                t.addCell(node != null ? node.getName() : null);
            } else {
                t.addCell(null);
                t.addCell(null);
            }
            t.endRow();
        }

        return t;
    }

    // ---- indices -------------------------------------------------------------------------------

    private void resolveIndices(ActionListener<Table> listener) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest(TimeValue.timeValueSeconds(30)).clear()
            .metadata(true)
            .routingTable(true)
            .indicesOptions(IndicesOptions.lenientExpandHidden());

        AtomicReference<ClusterStateResponse> clusterStateRef = new AtomicReference<>();
        AtomicReference<IndicesStatsResponse> indicesStatsRef = new AtomicReference<>();

        try (
            var listeners = new RefCountingListener(
                listener.map(ignored -> buildIndicesTable(clusterStateRef.get(), indicesStatsRef.get()))
            )
        ) {
            client.admin().cluster().state(clusterStateRequest, listeners.acquire(clusterStateRef::set));
            client.admin()
                .indices()
                .stats(
                    new IndicesStatsRequest().all().indicesOptions(IndicesOptions.lenientExpandHidden()),
                    listeners.acquire(indicesStatsRef::set)
                );
        }
    }

    private Table buildIndicesTable(ClusterStateResponse clusterStateResponse, IndicesStatsResponse indicesStatsResponse) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("health");
        t.addCell("status");
        t.addCell("index");
        t.addCell("uuid");
        t.addCell("pri");
        t.addCell("rep");
        t.addCell("docs.count");
        t.addCell("docs.deleted");
        t.addCell("store.size");
        t.addCell("dataset.size");
        t.endHeaders();

        ProjectMetadata project = projectResolver.getProjectMetadata(clusterStateResponse.getState());
        if (project == null) {
            return t;
        }

        Map<String, org.elasticsearch.action.admin.indices.stats.IndexStats> indexStatsMap = indicesStatsResponse.getIndices();

        project.indices().values().forEach(indexMetadata -> {
            String indexName = indexMetadata.getIndex().getName();
            var indexRoutingTable = clusterStateResponse.getState().routingTable(project.id()).index(indexName);

            String health;
            if (indexRoutingTable != null) {
                ClusterHealthStatus indexHealthStatus = new ClusterIndexHealth(indexMetadata, indexRoutingTable).getStatus();
                health = indexHealthStatus.toString().toLowerCase(Locale.ROOT);
            } else {
                health = "red";
            }

            var indexStats = indexStatsMap.get(indexName);
            CommonStats totalStats = (indexStats != null) ? indexStats.getTotal() : null;

            t.startRow();
            t.addCell(health);
            t.addCell(indexMetadata.getState().toString().toLowerCase(Locale.ROOT));
            t.addCell(indexName);
            t.addCell(indexMetadata.getIndexUUID());
            t.addCell(indexMetadata.getNumberOfShards());
            t.addCell(indexMetadata.getNumberOfReplicas());
            t.addCell(getStatLong(totalStats, s -> s.getDocs() != null ? s.getDocs().getCount() : null));
            t.addCell(getStatLong(totalStats, s -> s.getDocs() != null ? s.getDocs().getDeleted() : null));
            t.addCell(getStatLong(totalStats, s -> s.getStore() != null ? s.getStore().size().getBytes() : null));
            t.addCell(getStatLong(totalStats, s -> s.getStore() != null ? s.getStore().totalDataSetSize().getBytes() : null));
            t.endRow();
        });

        return t;
    }

    // ---- nodes ---------------------------------------------------------------------------------

    private void resolveNodes(ActionListener<Table> listener) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest(TimeValue.timeValueSeconds(30)).clear().nodes(true);

        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear()
            .addMetrics(
                NodesInfoMetrics.Metric.JVM.metricName(),
                NodesInfoMetrics.Metric.OS.metricName(),
                NodesInfoMetrics.Metric.PROCESS.metricName()
            );

        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
        nodesStatsRequest.setIncludeShardsStats(false);
        nodesStatsRequest.clear().addMetrics(NodesStatsRequestParameters.Metric.JVM, NodesStatsRequestParameters.Metric.OS);

        AtomicReference<ClusterStateResponse> clusterStateRef = new AtomicReference<>();
        AtomicReference<NodesInfoResponse> nodesInfoRef = new AtomicReference<>();
        AtomicReference<NodesStatsResponse> nodesStatsRef = new AtomicReference<>();

        try (
            var listeners = new RefCountingListener(
                listener.map(ignored -> buildNodesTable(clusterStateRef.get(), nodesInfoRef.get(), nodesStatsRef.get()))
            )
        ) {
            client.admin().cluster().state(clusterStateRequest, listeners.acquire(clusterStateRef::set));
            client.admin().cluster().nodesInfo(nodesInfoRequest, listeners.acquire(nodesInfoRef::set));
            client.admin().cluster().nodesStats(nodesStatsRequest, listeners.acquire(nodesStatsRef::set));
        }
    }

    private Table buildNodesTable(
        ClusterStateResponse clusterStateResponse,
        NodesInfoResponse nodesInfoResponse,
        NodesStatsResponse nodesStatsResponse
    ) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("ip");
        t.addCell("heap.percent");
        t.addCell("ram.percent");
        t.addCell("cpu");
        t.addCell("load_1m");
        t.addCell("load_5m");
        t.addCell("load_15m");
        t.addCell("node.role");
        t.addCell("master");
        t.addCell("name");
        t.endHeaders();

        String masterId = clusterStateResponse.getState().nodes().getMasterNodeId();

        for (DiscoveryNode node : clusterStateResponse.getState().nodes()) {
            NodeInfo info = nodesInfoResponse.getNodesMap().get(node.getId());
            NodeStats stats = nodesStatsResponse.getNodesMap().get(node.getId());

            JvmStats jvmStats = stats != null ? stats.getJvm() : null;
            OsStats osStats = stats != null ? stats.getOs() : null;

            t.startRow();
            t.addCell(node.getHostAddress());
            t.addCell(jvmStats != null ? (int) jvmStats.getMem().getHeapUsedPercent() : null);
            t.addCell(osStats != null && osStats.getMem() != null ? (int) osStats.getMem().getUsedPercent() : null);
            t.addCell(osStats != null ? (int) osStats.getCpu().getPercent() : null);
            double[] loadAvg = osStats != null ? osStats.getCpu().getLoadAverage() : null;
            t.addCell(formatLoad(loadAvg, 0));
            t.addCell(formatLoad(loadAvg, 1));
            t.addCell(formatLoad(loadAvg, 2));
            t.addCell(node.getRoleAbbreviationString());
            t.addCell(node.getId().equals(masterId) ? "*" : "-");
            t.addCell(node.getName());
            t.endRow();
        }

        return t;
    }

    // ---- allocation ----------------------------------------------------------------------------

    private void resolveAllocation(ActionListener<Table> listener) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest(TimeValue.timeValueSeconds(30)).clear()
            .nodes(true)
            .routingTable(true);

        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest().clear()
            .addMetric(NodesStatsRequestParameters.Metric.FS)
            .addMetric(NodesStatsRequestParameters.Metric.ALLOCATIONS);
        nodesStatsRequest.setIncludeShardsStats(true);

        ListenableFuture<ClusterStateResponse> clusterStateFuture = new ListenableFuture<>();
        client.admin().cluster().state(clusterStateRequest, clusterStateFuture);

        client.admin()
            .cluster()
            .nodesStats(
                nodesStatsRequest,
                listener.delegateFailure(
                    (delegate, nodesStatsResponse) -> clusterStateFuture.addListener(
                        delegate.map(clusterStateResponse -> buildAllocationTable(clusterStateResponse, nodesStatsResponse))
                    )
                )
            );
    }

    private static final String UNASSIGNED = "UNASSIGNED";

    private Table buildAllocationTable(ClusterStateResponse clusterStateResponse, NodesStatsResponse nodesStatsResponse) {
        Map<String, Integer> shardCounts = new HashMap<>();
        for (ShardRouting shard : clusterStateResponse.getState().routingTable().allShardsIterator()) {
            String nodeId = shard.assignedToNode() ? shard.currentNodeId() : UNASSIGNED;
            shardCounts.merge(nodeId, 1, Integer::sum);
        }

        Table t = new Table();
        t.startHeaders();
        t.addCell("shards");
        t.addCell("shards.undesired");
        t.addCell("write_load.forecast");
        t.addCell("disk.indices.forecast");
        t.addCell("disk.indices");
        t.addCell("disk.used");
        t.addCell("disk.avail");
        t.addCell("disk.total");
        t.addCell("disk.percent");
        t.addCell("host");
        t.addCell("ip");
        t.addCell("node");
        t.addCell("node.role");
        t.endHeaders();

        for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
            DiscoveryNode node = nodeStats.getNode();
            FsInfo fsInfo = nodeStats.getFs();
            NodeAllocationStats allocationStats = nodeStats.getNodeAllocationStats();

            Long totalBytes = null;
            Long availBytes = null;
            Long usedBytes = null;
            Integer diskPercent = null;

            if (fsInfo != null) {
                FsInfo.Path total = fsInfo.getTotal();
                if (total.getTotal().getBytes() >= 0) {
                    totalBytes = total.getTotal().getBytes();
                    availBytes = total.getAvailable().getBytes() >= 0 ? total.getAvailable().getBytes() : null;
                    if (totalBytes > 0 && availBytes != null) {
                        usedBytes = totalBytes - availBytes;
                        diskPercent = (int) (usedBytes * 100L / totalBytes);
                    }
                }
            }

            Long diskIndices = null;
            if (nodeStats.getIndices() != null && nodeStats.getIndices().getStore() != null) {
                StoreStats store = nodeStats.getIndices().getStore();
                diskIndices = store.size().getBytes();
            }

            t.startRow();
            t.addCell(shardCounts.getOrDefault(node.getId(), 0));
            t.addCell(allocationStats != null ? allocationStats.undesiredShards() : null);
            t.addCell(allocationStats != null ? allocationStats.forecastedIngestLoad() : null);
            t.addCell(allocationStats != null ? allocationStats.forecastedDiskUsage() : null);
            t.addCell(diskIndices);
            t.addCell(usedBytes);
            t.addCell(availBytes);
            t.addCell(totalBytes);
            t.addCell(diskPercent);
            t.addCell(node.getHostName());
            t.addCell(node.getHostAddress());
            t.addCell(node.getName());
            t.addCell(node.getRoleAbbreviationString());
            t.endRow();
        }

        if (shardCounts.containsKey(UNASSIGNED)) {
            t.startRow();
            t.addCell(shardCounts.get(UNASSIGNED));
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(null);
            t.addCell(UNASSIGNED);
            t.addCell(null);
            t.endRow();
        }

        return t;
    }

    // ---- helpers -------------------------------------------------------------------------------

    @FunctionalInterface
    private interface StatExtractor {
        Long extract(CommonStats stats);
    }

    private static Long getStatLong(CommonStats stats, StatExtractor extractor) {
        if (stats == null) return null;
        return extractor.extract(stats);
    }

    private static String formatLoad(double[] loadAvg, int index) {
        if (loadAvg == null || loadAvg.length <= index || loadAvg[index] == -1) {
            return "-";
        }
        return String.format(Locale.ROOT, "%.2f", loadAvg[index]);
    }
}
