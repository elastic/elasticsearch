/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class CcrRetentionLeases {

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> RETENTION_LEASE_RENEW_INTERVAL_SETTING =
            Setting.timeSetting(
                    "index.ccr.retention_lease.renew_interval",
                    new TimeValue(30, TimeUnit.SECONDS),
                    new TimeValue(0, TimeUnit.MILLISECONDS),
                    Setting.Property.NodeScope);

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

    /**
     * Synchronously requests to add a retention lease with the specified retention lease ID on the specified leader shard using the given
     * remote client. Note that this method will block up to the specified timeout.
     *
     * @param leaderShardId           the leader shard ID
     * @param retentionLeaseId        the retention lease ID
     * @param retainingSequenceNumber the retaining sequence number
     * @param remoteClient            the remote client on which to execute this request
     * @param timeout                 the timeout
     * @return an optional exception indicating whether or not the retention lease already exists
     */
    public static Optional<RetentionLeaseAlreadyExistsException> syncAddRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final TimeValue timeout) {
        try {
            final PlainActionFuture<RetentionLeaseActions.Response> response = new PlainActionFuture<>();
            asyncAddRetentionLease(leaderShardId, retentionLeaseId, retainingSequenceNumber, remoteClient, response);
            response.actionGet(timeout);
            return Optional.empty();
        } catch (final RetentionLeaseAlreadyExistsException e) {
            return Optional.of(e);
        }
    }

    /**
     * Asynchronously requests to add a retention lease with the specified retention lease ID on the specified leader shard using the given
     * remote client. Note that this method will return immediately, with the specified listener callback invoked to indicate a response
     * or failure.
     *
     * @param leaderShardId           the leader shard ID
     * @param retentionLeaseId        the retention lease ID
     * @param retainingSequenceNumber the retaining sequence number
     * @param remoteClient            the remote client on which to execute this request
     * @param listener                the listener
     */
    public static void asyncAddRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.AddRequest request =
                new RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, retainingSequenceNumber, "ccr");
        remoteClient.execute(RetentionLeaseActions.Add.INSTANCE, request, listener);
    }

    /**
     * Synchronously requests to renew a retention lease with the specified retention lease ID on the specified leader shard using the given
     * remote client. Note that this method will block up to the specified timeout.
     *
     * @param leaderShardId           the leader shard ID
     * @param retentionLeaseId        the retention lease ID
     * @param retainingSequenceNumber the retaining sequence number
     * @param remoteClient            the remote client on which to execute this request
     * @param timeout                 the timeout
     * @return an optional exception indicating whether or not the retention lease already exists
     */
    public static Optional<RetentionLeaseNotFoundException> syncRenewRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final TimeValue timeout) {
        try {
            final PlainActionFuture<RetentionLeaseActions.Response> response = new PlainActionFuture<>();
            asyncRenewRetentionLease(leaderShardId, retentionLeaseId, retainingSequenceNumber, remoteClient, response);
            response.actionGet(timeout);
            return Optional.empty();
        } catch (final RetentionLeaseNotFoundException e) {
            return Optional.of(e);
        }
    }

    /**
     * Asynchronously requests to renew a retention lease with the specified retention lease ID on the specified leader shard using the
     * given remote client. Note that this method will return immediately, with the specified listener callback invoked to indicate a
     * response or failure.
     *
     * @param leaderShardId           the leader shard ID
     * @param retentionLeaseId        the retention lease ID
     * @param retainingSequenceNumber the retaining sequence number
     * @param remoteClient            the remote client on which to execute this request
     * @param listener                the listener
     */
    public static void asyncRenewRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.RenewRequest request =
                new RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, retainingSequenceNumber, "ccr");
        remoteClient.execute(RetentionLeaseActions.Renew.INSTANCE, request, listener);
    }

    /**
     * Asynchronously requests to remove a retention lease with the specified retention lease ID on the specified leader shard using the
     * given remote client. Note that this method will return immediately, with the specified listener callback invoked to indicate a
     * response or failure.
     *
     * @param leaderShardId    the leader shard ID
     * @param retentionLeaseId the retention lease ID
     * @param remoteClient     the remote client on which to execute this request
     * @param listener         the listener
     */
    public static void asyncRemoveRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.RemoveRequest request = new RetentionLeaseActions.RemoveRequest(leaderShardId, retentionLeaseId);
        remoteClient.execute(RetentionLeaseActions.Remove.INSTANCE, request, listener);
    }

}
