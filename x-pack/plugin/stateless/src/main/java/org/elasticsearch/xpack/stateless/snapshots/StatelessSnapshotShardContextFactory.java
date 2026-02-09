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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.LocalPrimarySnapshotShardContextFactory;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A factory implementation for creating {@link StatelessSnapshotShardContext} instance that reads shard data from an object store.
 * Note this factory extends {@link LocalPrimarySnapshotShardContextFactory} so that it delegates to it when stateless snapshot
 * is disabled.
 */
public class StatelessSnapshotShardContextFactory implements SnapshotShardContextFactory {

    public enum StatelessSnapshotEnabledStatus {
        /**
         * The stateless snapshot functionality is disabled
         */
        DISABLED,

        /**
         * The stateless snapshot reads indices data from the object store. But otherwise no changes.
         */
        READ_FROM_OBJECT_STORE
    }

    public static final Setting<StatelessSnapshotEnabledStatus> STATELESS_SNAPSHOT_ENABLED_SETTING = Setting.enumSetting(
        StatelessSnapshotEnabledStatus.class,
        "stateless.snapshot.enabled",
        StatelessSnapshotEnabledStatus.DISABLED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(StatelessSnapshotShardContextFactory.class);

    private volatile StatelessSnapshotEnabledStatus statelessSnapshotEnabledStatus;
    private final StatelessCommitService commitService;
    private final BiFunction<ShardId, Long, BlobContainer> shardBlobContainerFunc;
    private final LocalPrimarySnapshotShardContextFactory delegateFactory;

    public StatelessSnapshotShardContextFactory() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessSnapshotShardContextFactory(StatelessPlugin stateless) {
        this.commitService = stateless.getCommitService();
        this.shardBlobContainerFunc = stateless.shardBlobContainerFunc();
        stateless.getClusterService()
            .getClusterSettings()
            .initializeAndWatch(STATELESS_SNAPSHOT_ENABLED_SETTING, this::setStatelessSnapshotEnabledStatus);
        this.delegateFactory = new LocalPrimarySnapshotShardContextFactory(stateless.getClusterService(), stateless.getIndicesService()) {
            @Override
            protected SubscribableListener<SnapshotShardContext> doAsyncCreate(
                ShardId shardId,
                Snapshot snapshot,
                IndexId indexId,
                IndexShardSnapshotStatus snapshotStatus,
                IndexVersion repositoryMetaVersion,
                long snapshotStartTime,
                ActionListener<ShardSnapshotResult> listener,
                IndexShard indexShard,
                SnapshotIndexCommit snapshotIndexCommit,
                String shardStateId
            ) {
                if (statelessSnapshotEnabledStatus == StatelessSnapshotEnabledStatus.DISABLED) {
                    return super.doAsyncCreate(
                        shardId,
                        snapshot,
                        indexId,
                        snapshotStatus,
                        repositoryMetaVersion,
                        snapshotStartTime,
                        listener,
                        indexShard,
                        snapshotIndexCommit,
                        shardStateId
                    );
                }

                assert statelessSnapshotEnabledStatus == StatelessSnapshotEnabledStatus.READ_FROM_OBJECT_STORE;
                logger.debug("stateless snapshot is enabled. Reading shard {} data from object store for snapshot [{}]", shardId, snapshot);
                final var indexCommit = snapshotIndexCommit.indexCommit();
                final long generation = indexCommit.getGeneration();
                final var snapshotShardContextListener = new SubscribableListener<SnapshotShardContext>();

                commitService.ensureMaxGenerationToUploadForFlush(shardId, generation);
                commitService.addListenerForUploadedGeneration(shardId, generation, ActionListener.wrap(ignore -> {
                    snapshotStatus.updateStatusDescription("commit uploaded, proceeding with snapshot");
                    snapshotStatus.ensureNotAborted();

                    final var metadataSnapshot = indexShard.store().getMetadata(indexCommit);
                    final Collection<String> fileNames = indexCommit.getFileNames();
                    final Map<String, BlobLocation> blobLocations = fileNames.stream()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                Function.identity(),
                                fileName -> Objects.requireNonNull(commitService.getBlobLocation(shardId, fileName))
                            )
                        );

                    snapshotShardContextListener.onResponse(
                        new StatelessSnapshotShardContext(
                            indexShard.shardId(),
                            snapshot.getSnapshotId(),
                            indexId,
                            shardStateId,
                            snapshotStatus,
                            repositoryMetaVersion,
                            snapshotStartTime,
                            metadataSnapshot,
                            blobLocations,
                            shardBlobContainerFunc,
                            snapshotIndexCommit.closingBefore(listener)
                        )
                    );
                }, exception -> {
                    snapshotIndexCommit.closingBefore(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {}

                        @Override
                        public void onFailure(Exception e) {
                            // we're already failing exceptionally, and prefer to propagate the original exception instead of this one
                            logger.warn(Strings.format("exception closing commit for [%s] in [%s]", shardId, snapshot), e);
                        }
                    }).onResponse(null);
                    snapshotShardContextListener.onFailure(exception);
                }));
                return snapshotShardContextListener;
            }
        };
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
        return delegateFactory.asyncCreate(shardId, snapshot, indexId, snapshotStatus, repositoryMetaVersion, snapshotStartTime, listener);
    }

    private void setStatelessSnapshotEnabledStatus(StatelessSnapshotEnabledStatus statelessSnapshotEnabledStatus) {
        logger.info("stateless snapshot enabled status is [{}]", statelessSnapshotEnabledStatus);
        this.statelessSnapshotEnabledStatus = statelessSnapshotEnabledStatus;
    }
}
