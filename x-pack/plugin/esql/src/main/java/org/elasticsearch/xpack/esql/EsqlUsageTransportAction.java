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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
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
import org.elasticsearch.xpack.esql.plugin.EsqlStatsAction;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsResponse;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class EsqlUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;

    @Inject
    public EsqlUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.ESQL.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.client = client;
    }

    @Override
    protected void masterOperation(
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
            EsqlFeatureSetUsage usage = new EsqlFeatureSetUsage(mergedCounters.toNestedMap());
            l.onResponse(new XPackUsageFeatureResponse(usage));
        }));
    }
}
