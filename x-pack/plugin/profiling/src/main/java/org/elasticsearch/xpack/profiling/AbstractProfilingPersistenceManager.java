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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public abstract class AbstractProfilingPersistenceManager<T extends AbstractProfilingPersistenceManager.ProfilingIndexAbstraction>
    implements
        ClusterStateListener,
        Closeable {
    protected final Logger logger = LogManager.getLogger(getClass());

    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final ClusterService clusterService;
    protected final ThreadPool threadPool;
    protected final Client client;
    private volatile boolean templatesEnabled;

    public AbstractProfilingPersistenceManager(ThreadPool threadPool, Client client, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
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

        if (event.state().nodes().getMaxNodeVersion().after(event.state().nodes().getSmallestNonClientNodeVersion())) {
            logger.debug("Skipping up-to-date check as cluster has mixed versions");
            return;
        }

        if (isAllResourcesCreated(event, clusterService.getSettings()) == false) {
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
                IndexState<T> state = getIndexState(clusterState, index);
                if (state.getStatus().actionable) {
                    onIndexState(clusterState, state, ActionListener.releasing(refs.acquire()));
                }
            }
        }
    }

    protected boolean isAllResourcesCreated(ClusterChangedEvent event, Settings settings) {
        return ProfilingIndexTemplateRegistry.isAllResourcesCreated(event.state(), settings);
    }

    /**
     * Extracts the appropriate index metadata for a given index from the cluster state.
     *
     * @param state Current cluster state. Never <code>null</code>.
     * @param index An index for which to retrieve index metadata. Never <code>null</code>.
     * @return The corresponding index metadata or <code>null</code> if there are none.
     */
    protected abstract IndexMetadata indexMetadata(ClusterState state, T index);

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

    private IndexState<T> getIndexState(ClusterState state, T index) {
        IndexMetadata metadata = indexMetadata(state, index);
        if (metadata == null) {
            return new IndexState<>(index, null, Status.NEEDS_CREATION);
        }
        if (metadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn(
                "Index [{}] is closed. This is likely to prevent Universal Profiling from functioning correctly",
                metadata.getIndex()
            );
            return new IndexState<>(index, metadata.getIndex(), Status.CLOSED);
        }
        final IndexRoutingTable routingTable = state.getRoutingTable().index(metadata.getIndex());
        ClusterHealthStatus indexHealth = new ClusterIndexHealth(metadata, routingTable).getStatus();
        if (indexHealth == ClusterHealthStatus.RED) {
            logger.trace("Index [{}] health status is RED, any pending mapping upgrades will wait until this changes", metadata.getIndex());
            return new IndexState<>(index, metadata.getIndex(), Status.UNHEALTHY);
        }
        MappingMetadata mapping = metadata.mapping();
        if (mapping != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> meta = (Map<String, Object>) mapping.sourceAsMap().get("_meta");
            int currentIndexVersion;
            int currentTemplateVersion;
            if (meta == null) {
                logger.debug("Missing _meta field in mapping of index [{}], assuming initial version.", metadata.getIndex());
                currentIndexVersion = 1;
                currentTemplateVersion = 1;
            } else {
                // we are extra defensive and treat any unexpected values as an unhealthy index which we won't touch.
                currentIndexVersion = getVersionField(metadata.getIndex(), meta, "index-version");
                currentTemplateVersion = getVersionField(metadata.getIndex(), meta, "index-template-version");
                if (currentIndexVersion == -1 || currentTemplateVersion == -1) {
                    return new IndexState<>(index, metadata.getIndex(), Status.UNHEALTHY);
                }
            }
            if (index.getVersion() > currentIndexVersion) {
                return new IndexState<>(index, metadata.getIndex(), Status.NEEDS_VERSION_BUMP);
            } else if (getIndexTemplateVersion() > currentTemplateVersion) {
                // if there are no migrations we can consider the index up-to-date even if the index template version does not match.
                List<Migration> pendingMigrations = index.getMigrations(currentTemplateVersion);
                if (pendingMigrations.isEmpty()) {
                    logger.trace(
                        "Index [{}] with index template version [{}] (current is [{}]) is up-to-date (no pending migrations).",
                        metadata.getIndex(),
                        currentTemplateVersion,
                        getIndexTemplateVersion()
                    );
                    return new IndexState<>(index, metadata.getIndex(), Status.UP_TO_DATE);
                }
                logger.trace(
                    "Index [{}] with index template version [{}] (current is [{}])  has [{}] pending migrations.",
                    metadata.getIndex(),
                    currentTemplateVersion,
                    getIndexTemplateVersion(),
                    pendingMigrations.size()
                );
                return new IndexState<>(index, metadata.getIndex(), Status.NEEDS_MAPPINGS_UPDATE, pendingMigrations);
            } else {
                return new IndexState<>(index, metadata.getIndex(), Status.UP_TO_DATE);
            }
        } else {
            logger.warn("No mapping found for existing index [{}]. Index cannot be migrated.", metadata.getIndex());
            return new IndexState<>(index, metadata.getIndex(), Status.UNHEALTHY);
        }
    }

    // overridable for testing
    protected int getIndexTemplateVersion() {
        return ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION;
    }

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

    private int getVersionField(Index index, Map<String, Object> meta, String fieldName) {
        Object value = meta.get(fieldName);
        if (value instanceof Integer) {
            return (int) value;
        }
        if (value == null) {
            logger.warn("Metadata version field [{}] of index [{}] is empty.", fieldName, index);
            return -1;
        }
        logger.warn("Metadata version field [{}] of index [{}] is [{}] (expected an integer).", fieldName, index, value);
        return -1;
    }

    protected static final class IndexState<T extends ProfilingIndexAbstraction> {
        private final T index;
        private final Index writeIndex;
        private final Status status;
        private final List<Migration> pendingMigrations;

        IndexState(T index, Index writeIndex, Status status) {
            this(index, writeIndex, status, null);
        }

        IndexState(T index, Index writeIndex, Status status, List<Migration> pendingMigrations) {
            this.index = index;
            this.writeIndex = writeIndex;
            this.status = status;
            this.pendingMigrations = pendingMigrations;
        }

        public T getIndex() {
            return index;
        }

        public Index getWriteIndex() {
            return writeIndex;
        }

        public Status getStatus() {
            return status;
        }

        public List<Migration> getPendingMigrations() {
            return pendingMigrations;
        }
    }

    enum Status {
        CLOSED(false),
        UNHEALTHY(false),
        NEEDS_CREATION(true),
        NEEDS_VERSION_BUMP(true),
        UP_TO_DATE(false),
        NEEDS_MAPPINGS_UPDATE(true);

        /**
         * Whether a status is for informational purposes only or whether it should be acted upon and may change cluster state.
         */
        private final boolean actionable;

        Status(boolean actionable) {
            this.actionable = actionable;
        }
    }

    /**
     * An index that is used by Universal Profiling.
     */
    interface ProfilingIndexAbstraction {
        String getName();

        int getVersion();

        List<Migration> getMigrations(int currentIndexTemplateVersion);
    }
}
