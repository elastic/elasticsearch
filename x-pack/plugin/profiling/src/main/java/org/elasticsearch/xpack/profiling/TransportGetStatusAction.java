/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;

public class TransportGetStatusAction extends TransportMasterNodeAction<GetStatusAction.Request, GetStatusAction.Response> {
    private static final Logger log = LogManager.getLogger(TransportGetStatusAction.class);

    private final StatusResolver resolver;

    @Inject
    public TransportGetStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetStatusAction.Request::new,
            indexNameExpressionResolver,
            GetStatusAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.resolver = new StatusResolver(clusterService);
    }

    @Override
    protected void masterOperation(
        Task task,
        GetStatusAction.Request request,
        ClusterState state,
        ActionListener<GetStatusAction.Response> listener
    ) {
        if (request.waitForResourcesCreated()) {
            createAndRegisterListener(listener, request.timeout());
        } else {
            listener.onResponse(resolver.getResponse(state));
        }
    }

    private void createAndRegisterListener(ActionListener<GetStatusAction.Response> listener, TimeValue timeout) {
        final DiscoveryNode localNode = clusterService.localNode();
        ClusterStateObserver.waitForState(
            clusterService,
            threadPool.getThreadContext(),
            new StatusListener(listener, localNode, clusterService, resolver),
            clusterState -> resolver.getResponse(clusterState).isResourcesCreated(),
            timeout,
            log
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetStatusAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    private static class StatusListener implements ClusterStateObserver.Listener {
        private final ActionListener<GetStatusAction.Response> listener;
        private final DiscoveryNode localNode;

        private final ClusterService clusterService;

        private final StatusResolver resolver;

        private StatusListener(
            ActionListener<GetStatusAction.Response> listener,
            DiscoveryNode localNode,
            ClusterService clusterService,
            StatusResolver resolver
        ) {
            this.listener = listener;
            this.localNode = localNode;
            this.clusterService = clusterService;
            this.resolver = resolver;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            listener.onResponse(resolver.getResponse(state));
        }

        @Override
        public void onClusterServiceClose() {
            listener.onFailure(new NodeClosedException(localNode));
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            GetStatusAction.Response response = resolver.getResponse(clusterService.state());
            response.setTimedOut(true);
            listener.onResponse(response);
        }
    }

    private static class StatusResolver {
        private final ClusterService clusterService;

        private StatusResolver(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        private GetStatusAction.Response getResponse(ClusterState state) {
            IndexStateResolver indexStateResolver = new IndexStateResolver(
                getValue(state, ProfilingPlugin.PROFILING_CHECK_OUTDATED_INDICES)
            );

            boolean pluginEnabled = getValue(state, XPackSettings.PROFILING_ENABLED);
            boolean resourceManagementEnabled = getValue(state, ProfilingPlugin.PROFILING_TEMPLATES_ENABLED);

            boolean templatesCreated = ProfilingIndexTemplateRegistry.isAllResourcesCreated(state, clusterService.getSettings());
            boolean indicesCreated = ProfilingIndexManager.isAllResourcesCreated(state, indexStateResolver);
            boolean dataStreamsCreated = ProfilingDataStreamManager.isAllResourcesCreated(state, indexStateResolver);
            boolean resourcesCreated = templatesCreated && indicesCreated && dataStreamsCreated;

            boolean indicesPre891 = ProfilingIndexManager.isAnyResourceTooOld(state, indexStateResolver);
            boolean dataStreamsPre891 = ProfilingDataStreamManager.isAnyResourceTooOld(state, indexStateResolver);
            boolean anyPre891Data = indicesPre891 || dataStreamsPre891;

            return new GetStatusAction.Response(pluginEnabled, resourceManagementEnabled, resourcesCreated, anyPre891Data);
        }

        private boolean getValue(ClusterState state, Setting<Boolean> setting) {
            Metadata metadata = state.getMetadata();
            if (metadata.settings().hasValue(setting.getKey())) {
                return setting.get(metadata.settings());
            } else {
                return setting.get(clusterService.getSettings());
            }
        }
    }
}
