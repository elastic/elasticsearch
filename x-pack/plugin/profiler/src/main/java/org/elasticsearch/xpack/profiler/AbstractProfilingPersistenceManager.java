/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractProfilingPersistenceManager<T extends AbstractProfilingPersistenceManager.ProfilingIndexAbstraction>
    implements
        ClusterStateListener,
        Closeable {
    protected final Logger logger = LogManager.getLogger(getClass());

    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    private final ClusterService clusterService;
    private volatile boolean templatesEnabled;

    public AbstractProfilingPersistenceManager(ClusterService clusterService) {
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

        if (isAllResourcesCreated(event) == false) {
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
                Status status = getStatus(clusterState, index);
                if (status.actionable) {
                    onStatus(clusterState, status, index, ActionListener.releasing(refs.acquire()));
                }
            }
        }
    }

    protected boolean isAllResourcesCreated(ClusterChangedEvent event) {
        return ProfilingIndexTemplateRegistry.isAllResourcesCreated(event.state());
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
     * @param status Status of the current index.
     * @param index The current index.
     * @param listener Listener to be called on completion / errors.
     */
    protected abstract void onStatus(ClusterState clusterState, Status status, T index, ActionListener<? super ActionResponse> listener);

    private Status getStatus(ClusterState state, T index) {
        IndexMetadata metadata = indexMetadata(state, index);
        if (metadata == null) {
            return Status.NEEDS_CREATION;
        }
        if (metadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn(
                "Index [{}] is closed. This is likely to prevent Universal Profiling from functioning correctly",
                metadata.getIndex()
            );
            return Status.CLOSED;
        }
        final IndexRoutingTable routingTable = state.getRoutingTable().index(metadata.getIndex());
        ClusterHealthStatus indexHealth = new ClusterIndexHealth(metadata, routingTable).getStatus();
        if (indexHealth == ClusterHealthStatus.RED) {
            logger.debug("Index [{}] health status is RED, any pending mapping upgrades will wait until this changes", metadata.getIndex());
            return Status.UNHEALTHY;
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
                    return Status.UNHEALTHY;
                }
            }
            if (index.getVersion() > currentIndexVersion) {
                return Status.NEEDS_VERSION_BUMP;
            } else if (ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION > currentTemplateVersion) {
                // TODO 8.10+: Check if there are any pending migrations. If none are pending we can consider the index up to date.
                return Status.NEEDS_MAPPINGS_UPDATE;
            } else {
                return Status.UP_TO_DATE;
            }
        } else {
            logger.warn("No mapping found for existing index [{}]. Index cannot be migrated.", metadata.getIndex());
            return Status.UNHEALTHY;
        }
    }

    private int getVersionField(Index index, Map<String, Object> meta, String fieldName) {
        Object value = meta.get(fieldName);
        if (value instanceof Integer) {
            return (int) value;
        } else if (value == null) {
            logger.warn("Metadata version field [{}] of index [{}] is empty.", fieldName, index);
            return -1;
        } else {
            logger.warn(
                "Expected metadata version field [{}] of index [{}] to contain an integer value but found [{}].",
                fieldName,
                index,
                value
            );
            return -1;
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
    }
}
