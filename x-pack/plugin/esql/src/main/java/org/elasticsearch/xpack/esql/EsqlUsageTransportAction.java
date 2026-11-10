/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.esql.EsqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.DataSourceInventoryCounters;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsAction;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsResponse;
import org.elasticsearch.xpack.esql.telemetry.QueryPath;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class EsqlUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;
    private final ProjectResolver projectResolver;
    private final DataSourceInventoryCounters inventoryCounters;

    public EsqlUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        ProjectResolver projectResolver
    ) {
        this(transportService, clusterService, threadPool, actionFilters, client, projectResolver, null);
    }

    @Inject
    public EsqlUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        ProjectResolver projectResolver,
        @Nullable DataSourceInventoryCounters inventoryCounters
    ) {
        super(XPackUsageFeatureAction.ESQL.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
        this.projectResolver = projectResolver;
        this.inventoryCounters = inventoryCounters == null ? new DataSourceInventoryCounters() : inventoryCounters;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {

        EsqlStatsRequest esqlRequest = new EsqlStatsRequest();
        esqlRequest.includeStats(true);
        esqlRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(EsqlStatsAction.INSTANCE, esqlRequest, listener.delegateFailureAndWrap((l, r) -> {
            List<Counters> countersPerNode = r.getNodes()
                .stream()
                .map(EsqlStatsResponse.NodeStatsResponse::getStats)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Counters mergedCounters = Counters.merge(countersPerNode);
            inventoryCounters.populate(projectResolver.getProjectMetadata(state), mergedCounters);
            EsqlFeatureSetUsage usage = new EsqlFeatureSetUsage(usageMap(mergedCounters));
            l.onResponse(new XPackUsageFeatureResponse(usage));
        }));
    }

    /**
     * The usage of the merged counters as nested maps, with two changes. Admission gauges are live occupancy for
     * {@code GET /_esql/stats}; phone-home usage is sampled after load, so those keys would always look idle and are dropped.
     * {@code query_paths} becomes a list with one object per path, so a telemetry indexer can store each path as one document
     * with its levels as fields, instead of one field per path.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> usageMap(Counters merged) {
        Map<String, Object> nested = merged.toNestedMap();
        Object datasources = nested.get("datasources");
        if (datasources instanceof Map<?, ?> ds) {
            ((Map<String, Object>) ds).remove("admission");
        }
        Object queryPaths = nested.get(QueryPath.USAGE_KEY);
        if (queryPaths instanceof Map<?, ?> paths) {
            nested.put(QueryPath.USAGE_KEY, queryPathList((Map<String, Object>) paths));
        }
        return nested;
    }

    /**
     * Turns {@code <caller>.<source>.<leading command>.{total, failed, commands}} into one object per path, sorted by caller,
     * source and leading command. Every object has {@code total}, {@code failed} and {@code commands}, even when zero or empty.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> queryPathList(Map<String, Object> byCaller) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (var caller : new TreeMap<>(byCaller).entrySet()) {
            for (var source : new TreeMap<>((Map<String, Object>) caller.getValue()).entrySet()) {
                for (var leading : new TreeMap<>((Map<String, Object>) source.getValue()).entrySet()) {
                    Map<String, Object> counts = (Map<String, Object>) leading.getValue();
                    Map<String, Object> path = new LinkedHashMap<>();
                    path.put(QueryPath.CALLER, caller.getKey());
                    path.put(QueryPath.SOURCE, source.getKey());
                    path.put(QueryPath.LEADING_COMMAND, leading.getKey());
                    path.put(QueryPath.TOTAL, counts.getOrDefault(QueryPath.TOTAL, 0L));
                    path.put(QueryPath.FAILED, counts.getOrDefault(QueryPath.FAILED, 0L));
                    path.put(QueryPath.COMMANDS, counts.getOrDefault(QueryPath.COMMANDS, Map.of()));
                    list.add(path);
                }
            }
        }
        return list;
    }

}
