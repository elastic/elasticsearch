/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointWithFanoutAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointWithFanoutAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointWithFanoutAction.Response;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;

public class TransportGetCheckpointWithFanoutAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetCheckpointWithFanoutAction.class);

    private final Client client;
    private final RemoteClusterService remoteClusterService;
    private final CrossProjectModeDecider crossProjectModeDecider;

    @Inject
    public TransportGetCheckpointWithFanoutAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        Settings settings
    ) {
        super(GetCheckpointWithFanoutAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.crossProjectModeDecider = new CrossProjectModeDecider(settings);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final IndicesOptions originalIndicesOptions = request.indicesOptions();
        final boolean resolveCrossProject = crossProjectModeDecider.resolvesCrossProject(request);
        final Map<String, OriginalIndices> indicesPerCluster = remoteClusterService.groupIndices(
            resolveCrossProject ? indicesOptionsForCrossProjectFanout(originalIndicesOptions) : originalIndicesOptions,
            request.indices()
        );
        final OriginalIndices localIndices = indicesPerCluster.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final ResolvedIndexExpressions localResolvedIndexExpressions = request.getResolvedIndexExpressions();

        final int totalRequests = indicesPerCluster.size() + (localIndices == null ? 0 : 1);
        if (totalRequests == 0) {
            listener.onResponse(new Response(Map.of(), localResolvedIndexExpressions));
            return;
        }

        final CountDown completionCounter = new CountDown(totalRequests);
        final SortedMap<String, Response> remoteResponses = Collections.synchronizedSortedMap(new TreeMap<>());
        final AtomicReference<Map<String, long[]>> localCheckpoints = new AtomicReference<>();
        final AtomicBoolean listenerNotified = new AtomicBoolean(false);
        final boolean hasRemoteClusters = indicesPerCluster.isEmpty() == false;

        final Runnable terminalHandler = () -> {
            if (completionCounter.countDown() == false || listenerNotified.get()) {
                return;
            }

            if (resolveCrossProject) {
                final Exception ex = CrossProjectIndexResolutionValidator.validate(
                    originalIndicesOptions,
                    request.getProjectRouting(),
                    localResolvedIndexExpressions,
                    getResolvedExpressionsByRemote(remoteResponses)
                );
                if (ex != null) {
                    if (listenerNotified.compareAndSet(false, true)) {
                        listener.onFailure(ex);
                    }
                    return;
                }
            }

            final Map<String, long[]> checkpoints = new TreeMap<>();
            Map<String, long[]> local = localCheckpoints.get();
            if (local != null) {
                checkpoints.putAll(local);
            }
            for (Map.Entry<String, Response> remoteResponse : remoteResponses.entrySet()) {
                String clusterAlias = remoteResponse.getKey();
                remoteResponse.getValue()
                    .getCheckpoints()
                    .forEach(
                        (index, checkpoint) -> checkpoints.put(
                            clusterAlias + RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR + index,
                            checkpoint
                        )
                    );
            }

            if (hasRemoteClusters) {
                listener.onResponse(new Response(checkpoints));
            } else {
                listener.onResponse(new Response(checkpoints, localResolvedIndexExpressions));
            }
        };

        if (localIndices != null) {
            GetCheckpointAction.Request localRequest = new GetCheckpointAction.Request(
                localIndices.indices(),
                localIndices.indicesOptions(),
                request.getQuery(),
                RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
                request.getTimeout()
            );
            client.execute(GetCheckpointAction.INSTANCE, localRequest, ActionListener.wrap(response -> {
                localCheckpoints.set(response.getCheckpoints());
                terminalHandler.run();
            }, failure -> {
                logger.info("failed to resolve checkpoints on local cluster", failure);
                if (listenerNotified.compareAndSet(false, true)) {
                    listener.onFailure(failure);
                }
                terminalHandler.run();
            }));
        }

        for (Map.Entry<String, OriginalIndices> remoteIndices : indicesPerCluster.entrySet()) {
            String clusterAlias = remoteIndices.getKey();
            OriginalIndices originalIndices = remoteIndices.getValue();
            var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                clusterAlias,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
            );
            Request remoteRequest = new Request(
                originalIndices.indices(),
                originalIndices.indicesOptions(),
                request.getQuery(),
                clusterAlias,
                request.getTimeout()
            );
            remoteClusterClient.execute(GetCheckpointWithFanoutAction.REMOTE_TYPE, remoteRequest, ActionListener.wrap(response -> {
                remoteResponses.put(clusterAlias, response);
                terminalHandler.run();
            }, failure -> {
                logger.info("failed to resolve checkpoints on remote cluster [" + clusterAlias + "]", failure);
                terminalHandler.run();
            }));
        }
    }

    private static Map<String, ResolvedIndexExpressions> getResolvedExpressionsByRemote(Map<String, Response> remoteResponses) {
        return remoteResponses.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            final ResolvedIndexExpressions resolvedIndexExpressions = e.getValue().getResolvedIndexExpressions();
            assert resolvedIndexExpressions != null
                : "remote response from cluster [" + e.getKey() + "] is missing resolved index expressions";
            return resolvedIndexExpressions;
        }));
    }
}
