/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexMappingUpdateService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Ensures the {@code .inference} system index exists and has up-to-date mappings before any write
 * operation is performed.
 *
 * <p>During a rolling upgrade the cluster can be in a "limbo" state where the index already exists
 * but carries older mappings while newer nodes are being brought up. Writing a document that references
 * fields introduced in a newer mapping version would fail with a mapping conflict. This class checks
 * the mapping version stored in {@code _meta.managed_index_mappings_version} on every write path and,
 * when the version is behind the descriptor's current version, issues a {@code CreateIndex} or
 * {@code PutMapping} request before allowing the write to proceed.
 *
 * <p>Concurrent callers are handled safely: the first caller that detects an update is needed acquires
 * an in-flight guard ({@link #updateInProgress}) and performs the update. Any additional callers that
 * arrive while the update is in progress are added to {@link #pendingListeners} and notified—with the
 * same success or failure outcome—when the in-flight update completes.
 */
public class InferenceIndexMappingManager {

    private static final Logger logger = LogManager.getLogger(InferenceIndexMappingManager.class);

    private final OriginSettingClient client;
    private final SystemIndexDescriptor descriptor;

    private volatile boolean updateInProgress = false;
    private final List<ActionListener<Void>> pendingListeners = new ArrayList<>();

    public InferenceIndexMappingManager(Client client, SystemIndexDescriptor descriptor) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
        this.descriptor = descriptor;
    }

    /**
     * Ensures the {@code .inference} index exists with up-to-date mappings, then notifies
     * {@code listener} with {@code null} on success, or with the failure exception on error.
     *
     * <p>If the index does not exist it is created. If it exists but has outdated mappings they are
     * updated before the listener is called. When an update is already in progress the listener is
     * queued and notified when the in-flight update finishes, avoiding redundant concurrent requests.
     *
     * @param clusterState the current cluster state used to inspect existing index metadata
     * @param listener     called with {@code null} on success, or with the failure exception
     */
    public void withUpToDateMappings(ClusterState clusterState, ActionListener<Void> listener) {
        var projectMetadata = clusterState.metadata().getProject();
        IndexMetadata indexMetadata = projectMetadata.index(descriptor.getPrimaryIndex());
        if (indexMetadata == null) {
            // The primary index name may have become an alias after a system index migration
            // (e.g. ".inference" → ".inference-reindexed-for-10"). ProjectMetadata.index() only
            // resolves concrete names, so we must fall back to the indices lookup, mirroring the
            // pattern used by SystemIndexMappingUpdateService.getSystemIndexMetadata().
            IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(descriptor.getPrimaryIndex());
            if (indexAbstraction != null && indexAbstraction.getWriteIndex() != null) {
                indexMetadata = projectMetadata.getIndexSafe(indexAbstraction.getWriteIndex());
            }
        }

        if (indexMetadata == null) {
            // Index does not exist yet – create it with the latest mappings.
            logger.debug("Index [{}] does not exist; creating it with up-to-date mappings", descriptor.getPrimaryIndex());
            startUpdateIfNotInProgress(listener, true);
            return;
        }

        // Index exists – check whether its mapping version is already current.
        if (SystemIndexMappingUpdateService.checkIndexMappingUpToDate(descriptor, indexMetadata)) {
            // Fast path: mappings are already up-to-date; call the listener immediately.
            listener.onResponse(null);
            return;
        }

        logger.debug(
            "Index [{}] has outdated mappings; updating to version {}",
            descriptor.getPrimaryIndex(),
            descriptor.getMappingsVersion().version()
        );
        startUpdateIfNotInProgress(listener, false);
    }

    /**
     * Acquires the update guard atomically. If an update is already in progress the listener is
     * queued and this method returns immediately. Otherwise, the appropriate index-level operation
     * (create or put-mapping) is issued.
     *
     * @param listener    the caller to notify when the update is complete
     * @param createIndex {@code true} to create the index, {@code false} to update mappings only
     */
    private void startUpdateIfNotInProgress(ActionListener<Void> listener, boolean createIndex) {
        synchronized (pendingListeners) {
            if (updateInProgress) {
                // An update is already in flight – enqueue this caller.
                logger.debug("Mapping update for [{}] already in progress; queuing listener", descriptor.getPrimaryIndex());
                pendingListeners.add(listener);
                return;
            }
            updateInProgress = true;
        }

        // We are the owner of this update; wrap the listener so pending callers are drained when done.
        ActionListener<Void> wrappedListener = ActionListener.wrap(v -> {
            try {
                listener.onResponse(null);
            } finally {
                drainPendingListeners(null);
            }
        }, e -> {
            try {
                listener.onFailure(e);
            } finally {
                drainPendingListeners(e);
            }
        });

        if (createIndex) {
            createIndex(wrappedListener);
        } else {
            putMapping(wrappedListener);
        }
    }

    /**
     * Clears the in-progress guard and notifies all queued listeners with the same outcome as the
     * completed update.
     *
     * @param exception the failure exception, or {@code null} if the update succeeded
     */
    private void drainPendingListeners(Exception exception) {
        List<ActionListener<Void>> toNotify;
        synchronized (pendingListeners) {
            updateInProgress = false;
            toNotify = new ArrayList<>(pendingListeners);
            pendingListeners.clear();
        }
        for (ActionListener<Void> pendingListener : toNotify) {
            try {
                if (exception != null) {
                    pendingListener.onFailure(exception);
                } else {
                    pendingListener.onResponse(null);
                }
            } catch (Exception e) {
                logger.warn("Listener threw an error while trying to drain pending listener", e);
            }
        }
    }

    private void createIndex(ActionListener<Void> listener) {
        String primaryIndex = descriptor.getPrimaryIndex();
        logger.debug("Creating index [{}] with up-to-date mappings", primaryIndex);
        CreateIndexRequest request = new CreateIndexRequest(primaryIndex).mapping(descriptor.getMappings())
            .settings(descriptor.getSettings())
            .alias(new Alias(descriptor.getAliasName()))
            .waitForActiveShards(ActiveShardCount.ALL);

        client.admin().indices().create(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                logger.debug("Successfully created index [{}]", primaryIndex);
                listener.onResponse(null);
            } else {
                logger.warn("Create index request for [{}] was not acknowledged", primaryIndex);
                listener.onFailure(new ElasticsearchException("Create index request for [" + primaryIndex + "] was not acknowledged"));
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                // Another node created the index while we were waiting; update mappings in case
                // it was created with an older version (e.g. by a node still running old code).
                logger.debug("Index [{}] already exists; updating mappings instead", primaryIndex);
                putMapping(listener);
            } else {
                logger.warn("Failed to create index [{}]", primaryIndex, e);
                listener.onFailure(e);
            }
        }));
    }

    private void putMapping(ActionListener<Void> listener) {
        String primaryIndex = descriptor.getPrimaryIndex();
        logger.debug("Updating mappings for index [{}] to version [{}]", primaryIndex, descriptor.getMappingsVersion().version());
        PutMappingRequest request = new PutMappingRequest(primaryIndex).source(descriptor.getMappings(), XContentType.JSON);

        client.admin().indices().putMapping(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                logger.debug("Successfully updated mappings for index [{}]", primaryIndex);
                listener.onResponse(null);
            } else {
                logger.warn("Put mapping request for [{}] was not acknowledged", primaryIndex);
                listener.onFailure(new ElasticsearchException("Put mapping request for [" + primaryIndex + "] was not acknowledged"));
            }
        }, e -> {
            logger.warn("Put mapping request for [{}] failed", primaryIndex, e);
            listener.onFailure(e);
        }));
    }
}
