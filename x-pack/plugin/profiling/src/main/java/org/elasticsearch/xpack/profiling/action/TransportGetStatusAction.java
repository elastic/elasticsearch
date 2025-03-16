/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.profiling.ProfilingPlugin;
import org.elasticsearch.xpack.profiling.persistence.EventsIndex;
import org.elasticsearch.xpack.profiling.persistence.IndexStateResolver;
import org.elasticsearch.xpack.profiling.persistence.ProfilingDataStreamManager;
import org.elasticsearch.xpack.profiling.persistence.ProfilingIndexManager;
import org.elasticsearch.xpack.profiling.persistence.ProfilingIndexTemplateRegistry;

public class TransportGetStatusAction extends TransportMasterNodeAction<GetStatusAction.Request, GetStatusAction.Response> {
    private static final Logger log = LogManager.getLogger(TransportGetStatusAction.class);

    private final StatusResolver resolver;

    @Inject
    public TransportGetStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        NodeClient nodeClient,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            GetStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetStatusAction.Request::new,
            GetStatusAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.resolver = new StatusResolver(clusterService, nodeClient);
    }

    @Override
    protected void masterOperation(
        Task task,
        GetStatusAction.Request request,
        ClusterState state,
        ActionListener<GetStatusAction.Response> listener
    ) {
        if (request.waitForResourcesCreated()) {
            createAndRegisterListener(listener, request.waitForResourcesCreatedTimeout());
        } else {
            resolver.execute(state, listener);
        }
    }

    private void createAndRegisterListener(ActionListener<GetStatusAction.Response> listener, TimeValue timeout) {
        final DiscoveryNode localNode = clusterService.localNode();
        ClusterStateObserver.waitForState(
            clusterService,
            threadPool.getThreadContext(),
            new StatusListener(listener, localNode, clusterService, resolver),
            resolver::isResourcesCreated,
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
            resolver.execute(state, listener);
        }

        @Override
        public void onClusterServiceClose() {
            listener.onFailure(new NodeClosedException(localNode));
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            resolver.execute(clusterService.state(), ActionListener.wrap(response -> {
                response.setTimedOut(true);
                listener.onResponse(response);
            }, listener::onFailure));
        }
    }

    private static class StatusResolver {
        private final ClusterService clusterService;
        private final NodeClient nodeClient;

        private StatusResolver(ClusterService clusterService, NodeClient nodeClient) {
            this.clusterService = clusterService;
            this.nodeClient = nodeClient;
        }

        private boolean isResourcesCreated(ClusterState state) {
            IndexStateResolver indexStateResolver = indexStateResolver(state);
            boolean templatesCreated = ProfilingIndexTemplateRegistry.isAllResourcesCreated(state, clusterService.getSettings());
            boolean indicesCreated = ProfilingIndexManager.isAllResourcesCreated(state, indexStateResolver);
            boolean dataStreamsCreated = ProfilingDataStreamManager.isAllResourcesCreated(state, indexStateResolver);
            return templatesCreated && indicesCreated && dataStreamsCreated;
        }

        private boolean isAnyPre891Data(ClusterState state) {
            IndexStateResolver indexStateResolver = indexStateResolver(state);
            boolean indicesPre891 = ProfilingIndexManager.isAnyResourceTooOld(state, indexStateResolver);
            boolean dataStreamsPre891 = ProfilingDataStreamManager.isAnyResourceTooOld(state, indexStateResolver);
            return indicesPre891 || dataStreamsPre891;
        }

        private IndexStateResolver indexStateResolver(ClusterState state) {
            return new IndexStateResolver(getValue(state, ProfilingPlugin.PROFILING_CHECK_OUTDATED_INDICES));
        }

        private void execute(ClusterState state, ActionListener<GetStatusAction.Response> listener) {
            boolean pluginEnabled = getValue(state, XPackSettings.PROFILING_ENABLED);
            boolean resourceManagementEnabled = getValue(state, ProfilingPlugin.PROFILING_TEMPLATES_ENABLED);
            boolean resourcesCreated = isResourcesCreated(state);
            boolean anyPre891Data = isAnyPre891Data(state);
            // only issue a search if there is any chance that we have data
            if (resourcesCreated) {
                SearchRequest countRequest = new SearchRequest(EventsIndex.FULL_INDEX.getName());
                countRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
                countRequest.allowPartialSearchResults(true);
                // we don't need an exact hit count, just whether there are any data at all
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true).trackTotalHitsUpTo(1);
                countRequest.source(searchSourceBuilder);

                nodeClient.search(countRequest, ActionListener.wrap(searchResponse -> {
                    boolean hasData = searchResponse.getHits().getTotalHits().value() > 0;
                    listener.onResponse(
                        new GetStatusAction.Response(pluginEnabled, resourceManagementEnabled, resourcesCreated, anyPre891Data, hasData)
                    );
                }, (e) -> {
                    // no data yet
                    if (e instanceof SearchPhaseExecutionException) {
                        log.trace("Has data check has failed.", e);
                        listener.onResponse(
                            new GetStatusAction.Response(pluginEnabled, resourceManagementEnabled, resourcesCreated, anyPre891Data, false)
                        );
                    } else {
                        listener.onFailure(e);
                    }
                }));
            } else {
                listener.onResponse(new GetStatusAction.Response(pluginEnabled, resourceManagementEnabled, false, anyPre891Data, false));
            }
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
