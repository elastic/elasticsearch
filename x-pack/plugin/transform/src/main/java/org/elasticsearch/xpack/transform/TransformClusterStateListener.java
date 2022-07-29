/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

class TransformClusterStateListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TransformClusterStateListener.class);

    private final Client client;
    private final AtomicBoolean isIndexCreationInProgress = new AtomicBoolean(false);

    TransformClusterStateListener(ClusterService clusterService, Client client) {
        this.client = client;
        clusterService.addListener(this);
        logger.debug("Created TransformClusterStateListener");
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }

        // The atomic flag prevents multiple simultaneous attempts to run alias creation
        // if there is a flurry of cluster state updates in quick succession
        if (event.localNodeMaster() && isIndexCreationInProgress.compareAndSet(false, true)) {
            createAuditAliasForDataFrameBWC(event.state(), client, ActionListener.wrap(r -> {
                isIndexCreationInProgress.set(false);
                if (r) {
                    logger.info("Created alias for deprecated data frame notifications index");
                } else {
                    logger.debug("Skipped creating alias for deprecated data frame notifications index");
                }
            }, e -> {
                isIndexCreationInProgress.set(false);
                logger.error("Error creating alias for deprecated data frame notifications index", e);
            }));
        }
    }

    private static void createAuditAliasForDataFrameBWC(ClusterState state, Client client, final ActionListener<Boolean> finalListener) {

        // check if old audit index exists, no need to create the alias if it does not
        if (state.getMetadata().hasIndexAbstraction(TransformInternalIndexConstants.AUDIT_INDEX_DEPRECATED) == false) {
            finalListener.onResponse(false);
            return;
        }

        Metadata metadata = state.metadata();
        if (state.getMetadata()
            .getIndicesLookup()
            .get(TransformInternalIndexConstants.AUDIT_INDEX_DEPRECATED)
            .getIndices()
            .stream()
            .anyMatch(name -> metadata.index(name).getAliases().containsKey(TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS))) {
            finalListener.onResponse(false);
            return;
        }

        final IndicesAliasesRequest request = client.admin()
            .indices()
            .prepareAliases()
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(TransformInternalIndexConstants.AUDIT_INDEX_DEPRECATED)
                    .alias(TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS)
                    .isHidden(true)
            )
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            TRANSFORM_ORIGIN,
            request,
            ActionListener.<AcknowledgedResponse>wrap(r -> finalListener.onResponse(r.isAcknowledged()), finalListener::onFailure),
            client.admin().indices()::aliases
        );
    }

}
