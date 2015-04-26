/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.snapshots;

import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.SnapshotFailedEngineException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreService;


/**
 * Shard level snapshot and restore service
 * <p/>
 * Performs snapshot and restore operations on the shard level.
 */
public class IndexShardSnapshotAndRestoreService extends AbstractIndexShardComponent {

    private final IndexShard indexShard;

    private final RepositoriesService repositoriesService;

    private final RestoreService restoreService;

    @Inject
    public IndexShardSnapshotAndRestoreService(ShardId shardId, @IndexSettings Settings indexSettings, IndexShard indexShard, RepositoriesService repositoriesService, RestoreService restoreService) {
        super(shardId, indexSettings);
        this.indexShard = indexShard;
        this.repositoriesService = repositoriesService;
        this.restoreService = restoreService;
    }

    /**
     * Creates shard snapshot
     *
     * @param snapshotId     snapshot id
     * @param snapshotStatus snapshot status
     */
    public void snapshot(final SnapshotId snapshotId, final IndexShardSnapshotStatus snapshotStatus) {
        IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(snapshotId.getRepository());
        if (!indexShard.routingEntry().primary()) {
            throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
        }
        if (indexShard.routingEntry().relocating()) {
            // do not snapshot when in the process of relocation of primaries so we won't get conflicts
            throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
        }
        if (indexShard.state() == IndexShardState.CREATED || indexShard.state() == IndexShardState.RECOVERING) {
            // shard has just been created, or still recovering
            throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
        }

        try {
            SnapshotIndexCommit snapshotIndexCommit = indexShard.snapshotIndex();
            try {
                indexShardRepository.snapshot(snapshotId, shardId, snapshotIndexCommit, snapshotStatus);
                if (logger.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("snapshot (").append(snapshotId.getSnapshot()).append(") completed to ").append(indexShardRepository).append(", took [").append(TimeValue.timeValueMillis(snapshotStatus.time())).append("]\n");
                    sb.append("    index    : version [").append(snapshotStatus.indexVersion()).append("], number_of_files [").append(snapshotStatus.numberOfFiles()).append("] with total_size [").append(new ByteSizeValue(snapshotStatus.totalSize())).append("]\n");
                    logger.debug(sb.toString());
                }
            } finally {
                snapshotIndexCommit.close();
            }
        } catch (SnapshotFailedEngineException e) {
            throw e;
        } catch (IndexShardSnapshotFailedException e) {
            throw e;
        } catch (Throwable e) {
            throw new IndexShardSnapshotFailedException(shardId, "Failed to snapshot", e);
        }
    }

    /**
     * Restores shard from {@link RestoreSource} associated with this shard in routing table
     *
     * @param recoveryState recovery state
     */
    public void restore(final RecoveryState recoveryState) {
        RestoreSource restoreSource = indexShard.routingEntry().restoreSource();
        if (restoreSource == null) {
            throw new IndexShardRestoreFailedException(shardId, "empty restore source");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] restoring shard  [{}]", restoreSource.snapshotId(), shardId);
        }
        try {
            recoveryState.getTranslog().totalOperations(0);
            recoveryState.getTranslog().totalOperationsOnStart(0);
            indexShard.prepareForIndexRecovery();
            IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(restoreSource.snapshotId().getRepository());
            ShardId snapshotShardId = shardId;
            if (!shardId.getIndex().equals(restoreSource.index())) {
                snapshotShardId = new ShardId(restoreSource.index(), shardId.id());
            }
            indexShardRepository.restore(restoreSource.snapshotId(), shardId, snapshotShardId, recoveryState);
            indexShard.skipTranslogRecovery();
            indexShard.finalizeRecovery();
            indexShard.postRecovery("restore done");
            restoreService.indexShardRestoreCompleted(restoreSource.snapshotId(), shardId);
        } catch (Throwable t) {
            if (Lucene.isCorruptionException(t)) {
                restoreService.failRestore(restoreSource.snapshotId(), shardId());
            }
            throw new IndexShardRestoreFailedException(shardId, "restore failed", t);
        }
    }

}
