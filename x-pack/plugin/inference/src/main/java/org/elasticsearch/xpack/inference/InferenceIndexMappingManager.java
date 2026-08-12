/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexMappingUpdateService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;

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
 * <p>Note that in a mixed-version cluster creating the index is not enough: managed system indices are
 * created with the mappings of the descriptor compatible with the minimum mappings version across all
 * nodes, which may be older than this node's latest. A successful create is therefore always followed
 * by a {@code PutMapping} carrying this node's latest mappings, which (as a cluster-generated,
 * origin-carrying request) is allowed to move the mappings ahead of an older master's descriptor.
 *
 * <p>Concurrent callers are handled safely: every caller that detects an update is needed subscribes
 * to a shared in-flight {@link SubscribableListener}; the first of them creates the listener and
 * performs the update. When the in-flight update completes, the same success or failure outcome is
 * fanned out to all subscribers.
 */
public class InferenceIndexMappingManager {

    private static final Logger logger = LogManager.getLogger(InferenceIndexMappingManager.class);

    private final OriginSettingClient client;
    private final SystemIndexDescriptor descriptor;

    @Nullable // non-null while a create/put-mapping update is in flight
    private SubscribableListener<Void> inFlightUpdate;

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
        // Resolving through SystemIndexMappingUpdateService also covers the case where the primary
        // index name has become an alias after a system index migration
        // (e.g. ".inference" → ".inference-reindexed-for-10").
        IndexMetadata indexMetadata = SystemIndexMappingUpdateService.getSystemIndexMetadata(
            clusterState.metadata().getProject(),
            descriptor
        );

        if (indexMetadata == null) {
            // Index does not exist yet – create it with the latest mappings.
            logger.debug("Index [{}] does not exist; creating it with up-to-date mappings", descriptor.getPrimaryIndex());
            startUpdateIfNotInProgress(this::createIndex, listener);
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
        startUpdateIfNotInProgress(l -> putMapping(true, l), listener);
    }

    /**
     * Subscribes the listener to the in-flight update, creating it if there is none. If an update is
     * already in progress this method returns after subscribing; the listener is notified when the
     * in-flight update completes. Otherwise, {@code updateAction} is issued and its outcome is fanned
     * out to all subscribers.
     *
     * @param updateAction the index-level operation (create or put-mapping) to run if this caller starts the update
     * @param listener     the caller to notify when the update is complete
     */
    private void startUpdateIfNotInProgress(CheckedConsumer<ActionListener<Void>, Exception> updateAction, ActionListener<Void> listener) {
        final SubscribableListener<Void> updateListener;
        final boolean startUpdate;
        synchronized (this) {
            if (inFlightUpdate == null) {
                inFlightUpdate = new SubscribableListener<>();
                startUpdate = true;
            } else {
                logger.debug(
                    () -> Strings.format("Mapping update for [%s] already in progress; queuing listener", descriptor.getPrimaryIndex())
                );
                startUpdate = false;
            }
            updateListener = inFlightUpdate;
        }

        // Restore the subscriber's thread context when it is notified: a queued caller would otherwise
        // run in the context of the caller that initiated the in-flight update, executing its follow-up
        // write under the wrong security/origin context.
        updateListener.addListener(listener, EsExecutors.DIRECT_EXECUTOR_SERVICE, client.threadPool().getThreadContext());

        if (startUpdate) {
            // Clear the in-flight guard before fanning out to subscribers so that a subscriber
            // re-entering withUpToDateMappings starts a fresh update rather than re-subscribing
            // to the completed one.
            ActionListener<Void> completionListener = ActionListener.runBefore(updateListener, () -> {
                synchronized (this) {
                    inFlightUpdate = null;
                }
            });
            // ActionListener.run routes an exception thrown synchronously by the update action to the
            // completion listener; otherwise the in-flight guard would never be cleared and every
            // subsequent caller would queue onto a listener that is never completed.
            ActionListener.run(completionListener, updateAction);
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
                // A successful create does NOT guarantee the index got this node's latest mappings:
                // for managed system indices, TransportCreateIndexAction ignores the mappings in the
                // request and applies the descriptor compatible with the minimum mappings version
                // across all nodes. In a mixed-version cluster that may be an older version, so we
                // always follow up with a put-mapping to bring the index to the latest mappings.
                logger.debug("Successfully created index [{}]; updating mappings to the latest version", primaryIndex);
            } else {
                // An unacknowledged create does not mean the index was not created: the master applied
                // the cluster state update but not every node acked it within the timeout (e.g. a busy
                // cluster). Proceed to the put-mapping, which the master resolves against its own,
                // up-to-date state, rather than failing a recoverable slow-cluster condition.
                logger.warn("Create index request for [{}] was not acknowledged; updating mappings anyway", primaryIndex);
            }
            putMapping(false, listener);
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof ResourceAlreadyExistsException || cause instanceof InvalidIndexNameException) {
                // ResourceAlreadyExistsException: another node created the index while we were waiting.
                // InvalidIndexNameException: MetadataCreateIndexService.validateIndexName throws
                // ResourceAlreadyExistsException only when the name is a concrete index; when it exists
                // as an alias (the post-system-index-migration case) it throws
                // InvalidIndexNameException("already exists as alias") instead. The primary index name
                // is a fixed, always-valid name, so this exception cannot mean the name is malformed.
                // Either way the index is there; update mappings in case it carries an older version.
                logger.debug("Index [{}] already exists; updating mappings instead", primaryIndex);
                putMapping(false, listener);
            } else {
                logger.warn("Failed to create index [{}]", primaryIndex, e);
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Issues a put-mapping for the primary index carrying the descriptor's latest mappings.
     *
     * @param fallBackToCreateOnMissingIndex whether an {@link IndexNotFoundException} should be handled
     *                                       by creating the index. This is {@code true} only for the
     *                                       top-level put-mapping path (the local cluster state said
     *                                       the index existed, but it may have been deleted before the
     *                                       request was processed); the put-mapping issued from
     *                                       {@link #createIndex} passes {@code false}, bounding the
     *                                       create/put-mapping fallback cycle to one round trip in
     *                                       each direction.
     * @param listener                       notified with the outcome
     */
    private void putMapping(boolean fallBackToCreateOnMissingIndex, ActionListener<Void> listener) {
        String primaryIndex = descriptor.getPrimaryIndex();
        logger.debug("Updating mappings for index [{}] to version [{}]", primaryIndex, descriptor.getMappingsVersion().version());
        // Setting the origin on the request itself exempts it from
        // TransportPutMappingAction.checkForSystemIndexViolations, which would otherwise reject the
        // request when this node's mappings differ from the master's descriptor (i.e. when the master
        // is an older node during a rolling upgrade). Cluster-generated requests are explicitly
        // permitted to differ so that rolling upgrade scenarios work; see also
        // ElasticsearchMappings.addDocMappingIfMissing which uses the same mechanism for ML indices.
        PutMappingRequest request = new PutMappingRequest(primaryIndex).source(descriptor.getMappings(), XContentType.JSON)
            .origin(ClientHelper.INFERENCE_ORIGIN);

        client.admin().indices().putMapping(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                logger.debug("Successfully updated mappings for index [{}]", primaryIndex);
                listener.onResponse(null);
            } else {
                // An unacknowledged put-mapping usually means a busy master; report it as retryable.
                logger.warn("Put mapping request for [{}] was not acknowledged", primaryIndex);
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Put mapping request for [" + primaryIndex + "] was not acknowledged",
                        RestStatus.TOO_MANY_REQUESTS
                    )
                );
            }
        }, e -> {
            if (fallBackToCreateOnMissingIndex && ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                // The local cluster state said the index existed, but it was deleted before the
                // put-mapping was processed (e.g. DELETE .inference, a feature-state reset, or a
                // system index migration in progress). Fall back to creating it.
                logger.debug("Index [{}] was deleted before the mapping update was processed; creating it instead", primaryIndex);
                createIndex(listener);
            } else {
                logger.warn("Put mapping request for [{}] failed", primaryIndex, e);
                listener.onFailure(e);
            }
        }));
    }
}
