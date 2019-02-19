/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import java.util.Locale;
import java.util.Optional;

import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;

public class CcrRetentionLeases {

    /**
     * The retention lease ID used by followers.
     *
     * @param localClusterName   the local cluster name
     * @param followerIndex      the follower index
     * @param remoteClusterAlias the remote cluster alias
     * @param leaderIndex        the leader index
     * @return the retention lease ID
     */
    public static String retentionLeaseId(
            final String localClusterName,
            final Index followerIndex,
            final String remoteClusterAlias,
            final Index leaderIndex) {
        return String.format(
                Locale.ROOT,
                "%s/%s/%s-following-%s/%s/%s",
                localClusterName,
                followerIndex.getName(),
                followerIndex.getUUID(),
                remoteClusterAlias,
                leaderIndex.getName(),
                leaderIndex.getUUID());
    }

    public static Optional<RetentionLeaseAlreadyExistsException> syncAddRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final Client remoteClient,
            final TimeValue timeout) {
        try {
            final PlainActionFuture<RetentionLeaseActions.Response> response = new PlainActionFuture<>();
            asyncAddRetentionLease(leaderShardId, retentionLeaseId, remoteClient, response);
            response.actionGet(timeout);
            return Optional.empty();
        } catch (final RetentionLeaseAlreadyExistsException e) {
            return Optional.of(e);
        }
    }

    public static void asyncAddRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.AddRequest request =
                new RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, RETAIN_ALL, "ccr");
        remoteClient.execute(RetentionLeaseActions.Add.INSTANCE, request, listener);
    }

    public static Optional<RetentionLeaseNotFoundException> syncRenewRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final Client remoteClient,
            final TimeValue timeout) {
        try {
            final PlainActionFuture<RetentionLeaseActions.Response> response = new PlainActionFuture<>();
            asyncRenewRetentionLease(leaderShardId, retentionLeaseId, remoteClient, response);
            response.actionGet(timeout);
            return Optional.empty();
        } catch (final RetentionLeaseNotFoundException e) {
            return Optional.of(e);
        }
    }

    public static void asyncRenewRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.RenewRequest request =
                new RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, RETAIN_ALL, "ccr");
        remoteClient.execute(RetentionLeaseActions.Renew.INSTANCE, request, listener);
    }

    public static void asyncRemoveRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.RemoveRequest request = new RetentionLeaseActions.RemoveRequest(leaderShardId, retentionLeaseId);
        remoteClient.execute(RetentionLeaseActions.Remove.INSTANCE, request, listener);
    }

}
