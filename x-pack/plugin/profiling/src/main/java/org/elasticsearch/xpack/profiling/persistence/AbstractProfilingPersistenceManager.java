/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.Closeable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

abstract class AbstractProfilingPersistenceManager<T extends ProfilingIndexAbstraction> implements ClusterStateListener, Closeable {
    protected final Logger logger = LogManager.getLogger(getClass());

    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final ClusterService clusterService;
    protected final ThreadPool threadPool;
    protected final Client client;
    private final IndexStateResolver indexStateResolver;
    private volatile boolean templatesEnabled;

    AbstractProfilingPersistenceManager(
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        IndexStateResolver indexStateResolver
    ) {
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
        this.indexStateResolver = indexStateResolver;
    }

    public void initialize() {
        clusterService.addListener(this);
    }

    @Override
    public void close() {
        clusterService.removeListener(this);
    }

    public void setTemplatesEnabled(boolean templatesEnabled) {
        this.templatesEnabled = templatesEnabled;
    }

    @Override
    public final void clusterChanged(ClusterChangedEvent event) {
        if (templatesEnabled == false) {
            return;
        }
        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // If this node is not a master node, exit.
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        if (event.state().nodes().isMixedVersionCluster()) {
            logger.debug("Skipping up-to-date check as cluster has mixed versions");
            return;
        }

        if (areAllIndexTemplatesCreated(event, clusterService.getSettings()) == false) {
            logger.trace("Skipping index creation; not all required resources are present yet");
            return;
        }

        if (inProgress.compareAndSet(false, true) == false) {
            logger.trace("Skipping index creation as changes are already in progress");
            return;
        }

        // Only release the lock once all upgrade attempts have succeeded or failed.
        try (var refs = new RefCountingRunnable(() -> inProgress.set(false))) {
            ClusterState clusterState = event.state();
            for (T index : getManagedIndices()) {
                IndexState<T> state = indexStateResolver.getIndexState(clusterState, index);
                if (state.getStatus().actionable) {
                    onIndexState(clusterState, state, ActionListener.releasing(refs.acquire()));
                } else if (state.getStatus() == IndexStatus.TOO_OLD) {
                    logger.info("Aborting index creation as index [{}] is considered too old.", index);
                    return;
                }
            }
        }
    }

    protected boolean areAllIndexTemplatesCreated(ClusterChangedEvent event, Settings settings) {
        return ProfilingIndexTemplateRegistry.isAllResourcesCreated(event.state(), settings);
    }

    /**
     * @return An iterable of all indices that are managed by this instance.
     */
    protected abstract Iterable<T> getManagedIndices();

    /**
     * Handler that takes appropriate action for a certain index status.
     *
     * @param clusterState The current cluster state. Never <code>null</code>.
     * @param indexState The state of the current index.
     * @param listener Listener to be called on completion / errors.
     */
    protected abstract void onIndexState(
        ClusterState clusterState,
        IndexState<T> indexState,
        ActionListener<? super ActionResponse> listener
    );

    protected final void applyMigrations(IndexState<T> indexState, ActionListener<? super ActionResponse> listener) {
        String writeIndex = indexState.getWriteIndex().getName();
        try (var refs = new RefCountingRunnable(() -> listener.onResponse(null))) {
            for (Migration migration : indexState.getPendingMigrations()) {
                logger.debug("Applying migration [{}] for [{}].", migration, writeIndex);
                migration.apply(
                    writeIndex,
                    (r -> updateMapping(r, ActionListener.releasing(refs.acquire()))),
                    (r -> updateSettings(r, ActionListener.releasing(refs.acquire())))
                );
            }
        }
    }

    protected final void updateMapping(PutMappingRequest request, ActionListener<AcknowledgedResponse> listener) {
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        executeAsync("put mapping", request, listener, (req, l) -> client.admin().indices().putMapping(req, l));
    }

    protected final void updateSettings(UpdateSettingsRequest request, ActionListener<AcknowledgedResponse> listener) {
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        executeAsync("update settings", request, listener, (req, l) -> client.admin().indices().updateSettings(req, l));
    }

    protected final <Request extends ActionRequest & IndicesRequest, Response extends AcknowledgedResponse> void executeAsync(
        final String actionName,
        final Request request,
        final ActionListener<Response> listener,
        BiConsumer<Request, ActionListener<Response>> consumer
    ) {
        final Executor executor = threadPool.generic();
        executor.execute(() -> {
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), ClientHelper.PROFILING_ORIGIN, request, new ActionListener<>() {
                @Override
                public void onResponse(Response response) {
                    if (response.isAcknowledged() == false) {
                        logger.error(
                            "Could not execute action [{}] for indices [{}] for [{}], request was not acknowledged",
                            actionName,
                            request.indices(),
                            ClientHelper.PROFILING_ORIGIN
                        );
                    }
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception ex) {
                    logger.error(
                        () -> format(
                            "Could not execute action [%s] for indices [%s] for [%s]",
                            actionName,
                            request.indices(),
                            ClientHelper.PROFILING_ORIGIN
                        ),
                        ex
                    );
                    listener.onFailure(ex);
                }
            }, consumer);
        });
    }
}
