/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class WatcherUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Client client;

    @Inject
    public WatcherUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        XPackLicenseState licenseState,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.WATCHER.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.enabled = XPackSettings.WATCHER_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        if (enabled) {
            ActionListener<XPackUsageFeatureResponse> preservingListener = ContextPreservingActionListener.wrapPreservingContext(
                listener,
                client.threadPool().getThreadContext()
            );
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(WATCHER_ORIGIN)) {
                WatcherStatsRequest statsRequest = new WatcherStatsRequest();
                statsRequest.includeStats(true);
                statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                client.execute(WatcherStatsAction.INSTANCE, statsRequest, ActionListener.wrap(r -> {
                    List<Counters> countersPerNode = r.getNodes()
                        .stream()
                        .map(WatcherStatsResponse.Node::getStats)
                        .filter(Objects::nonNull)
                        .toList();
                    Counters mergedCounters = Counters.merge(countersPerNode);
                    WatcherFeatureSetUsage usage = new WatcherFeatureSetUsage(
                        WatcherField.WATCHER_FEATURE.checkWithoutTracking(licenseState),
                        true,
                        mergedCounters.toNestedMap()
                    );
                    preservingListener.onResponse(new XPackUsageFeatureResponse(usage));
                }, preservingListener::onFailure));
            }
        } else {
            WatcherFeatureSetUsage usage = new WatcherFeatureSetUsage(
                WatcherField.WATCHER_FEATURE.checkWithoutTracking(licenseState),
                false,
                Collections.emptyMap()
            );
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }
}
