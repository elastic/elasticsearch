/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.eql.EqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.eql.plugin.EqlStatsAction;
import org.elasticsearch.xpack.eql.plugin.EqlStatsRequest;
import org.elasticsearch.xpack.eql.plugin.EqlStatsResponse;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class EqlUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;

    @Inject
    public EqlUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(XPackUsageFeatureAction.EQL.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {

        EqlStatsRequest eqlRequest = new EqlStatsRequest();
        eqlRequest.includeStats(true);
        eqlRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(EqlStatsAction.INSTANCE, eqlRequest, listener.delegateFailureAndWrap((delegate, r) -> {
            List<Counters> countersPerNode = r.getNodes()
                .stream()
                .map(EqlStatsResponse.NodeStatsResponse::getStats)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Counters mergedCounters = Counters.merge(countersPerNode);
            EqlFeatureSetUsage usage = new EqlFeatureSetUsage(mergedCounters.toNestedMap());
            delegate.onResponse(new XPackUsageFeatureResponse(usage));
        }));
    }
}
