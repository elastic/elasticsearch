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
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.LocalPrimarySnapshotShardContextFactory;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;

import java.io.IOException;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;

/**
 * A factory implementation for creating {@link StatelessSnapshotShardContext} instance that reads shard data from an object store.
 * Note this factory extends {@link LocalPrimarySnapshotShardContextFactory} so that it delegates to it when stateless snapshot
 * is disabled.
 */
public class StatelessSnapshotShardContextFactory implements SnapshotShardContextFactory {

    private static final Logger logger = LogManager.getLogger(StatelessSnapshotShardContextFactory.class);

    private volatile StatelessSnapshotEnabledStatus statelessSnapshotEnabledStatus;
    private final BiFunction<ShardId, Long, BlobContainer> shardBlobContainerFunc;
    private final SnapshotsCommitService snapshotsCommitService;
    private final LocalPrimarySnapshotShardContextFactory localPrimaryFactory;
    private final Client client;

    public StatelessSnapshotShardContextFactory() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessSnapshotShardContextFactory(StatelessPlugin stateless) {
        this.shardBlobContainerFunc = stateless.shardBlobContainerFunc();
        this.snapshotsCommitService = stateless.getSnapshotsCommitService();
        this.client = stateless.getClient();
        stateless.getClusterService()
            .getClusterSettings()
            .initializeAndWatch(STATELESS_SNAPSHOT_ENABLED_SETTING, this::setStatelessSnapshotEnabledStatus);
        this.localPrimaryFactory = new LocalPrimarySnapshotShardContextFactory(
            stateless.getClusterService(),
            stateless.getIndicesService()
        );
    }

    @Override
    public SubscribableListener<SnapshotShardContext> asyncCreate(
        ShardId shardId,
        Snapshot snapshot,
        IndexId indexId,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) throws IOException {
        final var status = statelessSnapshotEnabledStatus; // read volatile once
        if (status == StatelessSnapshotEnabledStatus.DISABLED) {
            return localPrimaryFactory.asyncCreate(
                shardId,
                snapshot,
                indexId,
                snapshotStatus,
                repositoryMetaVersion,
                snapshotStartTime,
                listener
            );
        } else {
            logger.debug("{} acquiring commit info for snapshot [{}], enabled status [{}]", shardId, snapshot, status.description());
            try {
                final var snapshotCommitInfo = snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(
                    shardId,
                    snapshot,
                    status.supportsRelocationDuringSnapshot(),
                    snapshotStatus
                );
                return SubscribableListener.newSucceeded(
                    new StatelessSnapshotShardContext(
                        shardId,
                        snapshot.getSnapshotId(),
                        indexId,
                        snapshotCommitInfo.shardStateId(),
                        snapshotStatus,
                        repositoryMetaVersion,
                        snapshotStartTime,
                        snapshotCommitInfo.snapshotIndexCommit(),
                        snapshotCommitInfo.blobLocations(),
                        shardBlobContainerFunc,
                        snapshotCommitInfo.snapshotIndexCommit().closingBefore(listener)
                    )
                );
            } catch (Exception e) {
                if (status.supportsRelocationDuringSnapshot() && TransportActions.isShardNotAvailableException(e)) {
                    logger.debug("{} became unavailable ({}) on starting snapshot, retry on remote node.", shardId, e.getMessage());
                    return asyncCreateOnRemoteNode(
                        shardId,
                        snapshot,
                        indexId,
                        snapshotStatus,
                        repositoryMetaVersion,
                        snapshotStartTime,
                        listener
                    );
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public boolean supportsRelocationDuringSnapshot() {
        return statelessSnapshotEnabledStatus.supportsRelocationDuringSnapshot();
    }

    private void setStatelessSnapshotEnabledStatus(StatelessSnapshotEnabledStatus statelessSnapshotEnabledStatus) {
        logger.info("stateless snapshot enabled status is [{}]", statelessSnapshotEnabledStatus);
        this.statelessSnapshotEnabledStatus = statelessSnapshotEnabledStatus;
    }

    private SubscribableListener<SnapshotShardContext> asyncCreateOnRemoteNode(
        ShardId shardId,
        Snapshot snapshot,
        IndexId indexId,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) {
        final var snapshotShardContextListener = new SubscribableListener<SnapshotShardContext>();
        snapshotStatus.updateStatusDescription("fetching commit info from primary shard");
        client.projectClient(snapshot.getProjectId())
            .execute(
                TransportGetShardSnapshotCommitInfoAction.TYPE,
                new GetShardSnapshotCommitInfoRequest(shardId, snapshot),
                snapshotShardContextListener.delegateFailureAndWrap((l, response) -> {
                    snapshotStatus.updateStatusDescription("commit info received from primary, proceeding with snapshot");
                    snapshotStatus.ensureNotAborted();
                    l.onResponse(
                        new StatelessSnapshotShardContext(
                            shardId,
                            snapshot.getSnapshotId(),
                            indexId,
                            response.shardStateId(),
                            snapshotStatus,
                            repositoryMetaVersion,
                            snapshotStartTime,
                            null,
                            response.blobLocations(),
                            shardBlobContainerFunc,
                            listener
                        )
                    );
                })
            );
        return snapshotShardContextListener;
    }
}
