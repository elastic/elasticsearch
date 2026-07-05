/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import static org.elasticsearch.core.Strings.format;

public class RecoveryCommitRegistrationHandler {

    private static final Logger logger = LogManager.getLogger(RecoveryCommitRegistrationHandler.class);

    private final Client client;
    private final ClusterService clusterService;

    public RecoveryCommitRegistrationHandler(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public void register(
        PrimaryTermAndGeneration batchedCompoundCommitPrimaryTermAndGeneration,
        PrimaryTermAndGeneration compoundCommitPrimaryTermAndGeneration,
        ShardId shardId,
        ActionListener<RegisterCommitResponse> listener
    ) {
        var request = new RegisterCommitRequest(
            batchedCompoundCommitPrimaryTermAndGeneration,
            compoundCommitPrimaryTermAndGeneration,
            shardId,
            clusterService.localNode().getId()
        );
        client.execute(TransportSendRecoveryCommitRegistrationAction.TYPE, request, new ActionListener<>() {
            @Override
            public void onResponse(RegisterCommitResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> format("%s error while registering %s for recovery", shardId, request), e);
                listener.onFailure(e);
            }
        });
    }
}
