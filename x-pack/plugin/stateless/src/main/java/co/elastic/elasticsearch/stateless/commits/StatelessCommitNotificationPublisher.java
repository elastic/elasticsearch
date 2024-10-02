/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;

import java.util.Set;

/**
 * Handles communication with the search nodes about what stateless
 * commits are available (new commits) and still in use (old commits).
 */
public class StatelessCommitNotificationPublisher {
    private static final Logger logger = LogManager.getLogger(StatelessCommitNotificationPublisher.class);
    private final Client client;

    StatelessCommitNotificationPublisher(Client client) {
        this.client = client;
    }

    /**
     * Broadcasts notification of a new uploaded BCC to all the search nodes hosting a replica shard for the given shard commit.
     */
    public void sendNewUploadedCommitNotification(
        IndexShardRoutingTable shardRoutingTable,
        BatchedCompoundCommit uploadedBcc,
        long clusterStateVersion,
        String localNodeId,
        ActionListener<Set<PrimaryTermAndGeneration>> listener
    ) {
        assert uploadedBcc != null;

        NewCommitNotificationRequest request = new NewCommitNotificationRequest(
            shardRoutingTable,
            uploadedBcc.lastCompoundCommit(),
            uploadedBcc.primaryTermAndGeneration().generation(),
            uploadedBcc.primaryTermAndGeneration(),
            clusterStateVersion,
            localNodeId
        );
        assert request.isUploaded();
        client.execute(
            TransportNewCommitNotificationAction.TYPE,
            request,
            listener.map(response -> response.getPrimaryTermAndGenerationsInUse())
        );
    }

    // TODO: merge this method with sendNewUploadedCommitNotification
    public void sendNewCommitNotification(
        IndexShardRoutingTable shardRoutingTable,
        StatelessCompoundCommit lastCompoundCommit,
        long batchedCompoundCommitGeneration,
        PrimaryTermAndGeneration maxUploadedBccTermAndGen,
        long clusterStateVersion,
        String localNodeId,
        ActionListener<Set<PrimaryTermAndGeneration>> listener
    ) {
        final var request = new NewCommitNotificationRequest(
            shardRoutingTable,
            lastCompoundCommit,
            batchedCompoundCommitGeneration,
            maxUploadedBccTermAndGen,
            clusterStateVersion,
            localNodeId
        );

        client.execute(
            TransportNewCommitNotificationAction.TYPE,
            request,
            listener.map(response -> response.getPrimaryTermAndGenerationsInUse())
        );
    }
}
