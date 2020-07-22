/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.eql.EqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.eql.plugin.EqlStatsAction;
import org.elasticsearch.xpack.eql.plugin.EqlStatsRequest;
import org.elasticsearch.xpack.eql.plugin.EqlStatsResponse;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class EqlUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Client client;

    @Inject
    public EqlUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   Settings settings, XPackLicenseState licenseState, Client client) {
        super(XPackUsageFeatureAction.EQL.name(), transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver);
        this.enabled = EqlPlugin.isEnabled(settings);
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        boolean available = licenseState.isAllowed(XPackLicenseState.Feature.EQL);
        if (enabled) {
            EqlStatsRequest eqlRequest = new EqlStatsRequest();
            eqlRequest.includeStats(true);
            eqlRequest.setParentTask(clusterService.localNode().getId(), task.getId());
            client.execute(EqlStatsAction.INSTANCE, eqlRequest, ActionListener.wrap(r -> {
                List<Counters> countersPerNode = r.getNodes()
                    .stream()
                    .map(EqlStatsResponse.NodeStatsResponse::getStats)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                Counters mergedCounters = Counters.merge(countersPerNode);
                EqlFeatureSetUsage usage = new EqlFeatureSetUsage(available, enabled, mergedCounters.toNestedMap());
                listener.onResponse(new XPackUsageFeatureResponse(usage));
            }, listener::onFailure));
        } else {
            EqlFeatureSetUsage usage = new EqlFeatureSetUsage(available, enabled, Collections.emptyMap());
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }
}
