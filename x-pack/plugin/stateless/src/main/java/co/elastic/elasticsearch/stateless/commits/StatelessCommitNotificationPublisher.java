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

import co.elastic.elasticsearch.stateless.action.FetchShardCommitsInUseAction;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportFetchShardCommitsInUseAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;

import java.util.Set;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.COMMIT_NOTIFICATION_TRANSPORT_ACTION_SPLIT;

/**
 * Handles communication with the search nodes about what stateless commits are available (new commits) and still in use (old commits).
 * Sends requests at the shard level.
 */
public class StatelessCommitNotificationPublisher {
    private static final Logger logger = LogManager.getLogger(StatelessCommitNotificationPublisher.class);
    private final Client client;

    /**
     * @param allSearchNodes All of the nodes that were contacted for in-use shard commits: current and old search node owners of the shard.
     * @param commitsInUse Aggregation of the commits that are still in-use by the collection of {@code allSearchNodes}.
     */
    record CommitsInUse(Set<String> allSearchNodes, Set<PrimaryTermAndGeneration> commitsInUse) {}

    StatelessCommitNotificationPublisher(Client client) {
        this.client = client;
    }

    /**
     * Broadcasts notification of a new uploaded BCC to all the search nodes hosting a replica shard for the given shard commit.
     * Also fetches any commits in-use from current and oldSearchNodes (if not empty). Returns a {@link CommitsInUse}.
     *
     * @param shardRoutingTable The current shard routing table.
     * @param currentRoutingNodesWithAssignedSearchShards The search nodes currently in the routing table for the shard.
     * @param allSearchNodesRetainingCommits Any old search nodes that currently or previously owned a search shard copy.
     * @param uploadedBcc
     * @param clusterStateVersion
     * @param localNodeId The local node ID.
     * @param clusterService Needed to check the transport version.
     * @param listener What should be done with the result ({@link CommitsInUse}).
     */
    public void sendNewUploadedCommitNotificationAndFetchInUseCommits(
        @Nullable IndexShardRoutingTable shardRoutingTable,
        Set<String> currentRoutingNodesWithAssignedSearchShards,
        Set<String> allSearchNodesRetainingCommits,
        BatchedCompoundCommit uploadedBcc,
        long clusterStateVersion,
        String localNodeId,
        ClusterService clusterService,
        ActionListener<CommitsInUse> listener
    ) {
        assert uploadedBcc != null;
        assert (shardRoutingTable == null && currentRoutingNodesWithAssignedSearchShards.isEmpty())
            || shardRoutingTable.unpromotableShards()
                .stream()
                .filter(ShardRouting::assignedToNode)
                .map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet())
                .equals(currentRoutingNodesWithAssignedSearchShards);

        if (clusterService.state().getMinTransportVersion().before(COMMIT_NOTIFICATION_TRANSPORT_ACTION_SPLIT)) {
            assert currentRoutingNodesWithAssignedSearchShards.equals(allSearchNodesRetainingCommits);
        }

        Set<String> oldSearchNodesRetainingCommits = Sets.difference(
            allSearchNodesRetainingCommits,
            currentRoutingNodesWithAssignedSearchShards
        );

        // Simultaneously send out TransportNewCommitNotificationAction and TransportFetchShardCommitsInUseAction requests, then
        // gather the results for the listener.
        SetOnce<Set<PrimaryTermAndGeneration>> newCommitNotificationResponse = new SetOnce<>();
        SetOnce<Set<PrimaryTermAndGeneration>> fetchShardCommitsInUseResponse = new SetOnce<>();
        try (var listeners = new RefCountingListener(listener.map(unused -> {
            assert newCommitNotificationResponse.get() != null;
            assert fetchShardCommitsInUseResponse.get() != null;

            // Aggregate the results of the two network requests.
            return new CommitsInUse(
                allSearchNodesRetainingCommits,
                Sets.union(newCommitNotificationResponse.get(), fetchShardCommitsInUseResponse.get())
            );
        }))) {

            // TODO (ES-9638): no need to run this request if the routing table has no search nodes.
            NewCommitNotificationRequest notificationRequest = new NewCommitNotificationRequest(
                shardRoutingTable,
                uploadedBcc.lastCompoundCommit(),
                uploadedBcc.primaryTermAndGeneration().generation(),
                uploadedBcc.primaryTermAndGeneration(),
                clusterStateVersion,
                localNodeId
            );
            assert notificationRequest.isUploaded();

            client.execute(
                TransportNewCommitNotificationAction.TYPE,
                notificationRequest,
                listeners.acquire(response -> newCommitNotificationResponse.set(response.getPrimaryTermAndGenerationsInUse()))
            );

            if (oldSearchNodesRetainingCommits.isEmpty()) {
                // Not running the Fetch action, no old search nodes to which to send requests.
                fetchShardCommitsInUseResponse.set(Set.of());
                return;
            }

            // Fetch in-use commits from old search nodes (nodes that no longer own a shard but may still be running search ops).

            FetchShardCommitsInUseAction.Request fetchRequest = new FetchShardCommitsInUseAction.Request(
                oldSearchNodesRetainingCommits.toArray(String[]::new),
                uploadedBcc.shardId()
            );

            client.execute(
                TransportFetchShardCommitsInUseAction.TYPE,
                fetchRequest,
                listeners.acquire(
                    oldSearchNodesResponse -> fetchShardCommitsInUseResponse.set(
                        oldSearchNodesResponse.getAllPrimaryTermAndGenerationsInUse()
                    )
                )
            );
        }
    }

    // TODO: Merge this method with sendNewUploadedCommitNotificationAndFetchInUseCommits.
    // Ultimately this caller should handle and process the response.
    public void sendNewCommitNotification(
        IndexShardRoutingTable shardRoutingTable,
        StatelessCompoundCommit lastCompoundCommit,
        long batchedCompoundCommitGeneration,
        PrimaryTermAndGeneration maxUploadedBccTermAndGen,
        long clusterStateVersion,
        String localNodeId,
        ActionListener<Void> listener
    ) {
        final var request = new NewCommitNotificationRequest(
            shardRoutingTable,
            lastCompoundCommit,
            batchedCompoundCommitGeneration,
            maxUploadedBccTermAndGen,
            clusterStateVersion,
            localNodeId
        );

        client.execute(TransportNewCommitNotificationAction.TYPE, request, listener.map(response -> null));
    }
}
