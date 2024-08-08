/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.watcher.Watcher;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class LocalStateMonitoring extends LocalStateCompositeXPackPlugin {

    public static class MonitoringTransportXPackUsageAction extends TransportXPackUsageAction {
        @Inject
        public MonitoringTransportXPackUsageAction(
            ThreadPool threadPool,
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            NodeClient client
        ) {
            super(threadPool, transportService, clusterService, actionFilters, indexNameExpressionResolver, client);
        }

        @Override
        protected List<ActionType<XPackUsageFeatureResponse>> usageActions() {
            return Collections.singletonList(XPackUsageFeatureAction.MONITORING);
        }
    }

    final Monitoring monitoring;

    public LocalStateMonitoring(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateMonitoring thisVar = this;

        monitoring = new Monitoring(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected LicenseService getLicenseService() {
                return thisVar.getLicenseService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        };
        plugins.add(monitoring);
        plugins.add(new Watcher(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
        plugins.add(new IndexLifecycle(settings));
        plugins.add(new DataStreamsPlugin(settings)); // Otherwise the watcher history index template can't be added
    }

    public Monitoring getMonitoring() {
        return monitoring;
    }

    @Override
    protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
        return MonitoringTransportXPackUsageAction.class;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var actions = super.getActions();
        // ccr StatsCollector
        actions.add(new ActionHandler<>(CcrStatsAction.INSTANCE, TransportCcrStatsStubAction.class));
        // For EnrichStatsCollector:
        actions.add(new ActionHandler<>(EnrichStatsAction.INSTANCE, TransportEnrichStatsStubAction.class));
        return actions;
    }

    public static class TransportCcrStatsStubAction extends HandledTransportAction<CcrStatsAction.Request, CcrStatsAction.Response> {

        @Inject
        public TransportCcrStatsStubAction(TransportService transportService, ActionFilters actionFilters) {
            super(CcrStatsAction.NAME, transportService, actionFilters, CcrStatsAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        @Override
        protected void doExecute(Task task, CcrStatsAction.Request request, ActionListener<CcrStatsAction.Response> listener) {
            AutoFollowStats autoFollowStats = new AutoFollowStats(
                0,
                0,
                0,
                Collections.emptyNavigableMap(),
                Collections.emptyNavigableMap()
            );
            FollowStatsAction.StatsResponses statsResponses = new FollowStatsAction.StatsResponses(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList()
            );
            listener.onResponse(new CcrStatsAction.Response(autoFollowStats, statsResponses));
        }
    }

    public static class TransportEnrichStatsStubAction extends HandledTransportAction<
        EnrichStatsAction.Request,
        EnrichStatsAction.Response> {

        @Inject
        public TransportEnrichStatsStubAction(TransportService transportService, ActionFilters actionFilters) {
            super(
                EnrichStatsAction.NAME,
                transportService,
                actionFilters,
                EnrichStatsAction.Request::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        @Override
        protected void doExecute(Task task, EnrichStatsAction.Request request, ActionListener<EnrichStatsAction.Response> listener) {
            listener.onResponse(new EnrichStatsAction.Response(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
        }
    }

}
