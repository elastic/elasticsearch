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

package org.elasticsearch.snapshots;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Service responsible for creating snapshots
 * <p/>
 * This serivce is relying on
 * <ul>
 * <li>{@link RepositoriesService} to obtain metadata</li>,
 * <li>{@link SnapshotManager} to start and abort individual snapshots</li>
 * <li>{@link SnapshotsShardService} to start snapshots on the shard level</li>
 * <li>{@link SnapshotsShardWatcherService} to monitor cluster state for the changes that affect the running snapshots</li>
 * </ul>
 * ,
 * <p/>
 * A typical snapshot creating process looks like this:
 * <ul>
 * <li>On the master node the {@link #createSnapshot(SnapshotRequest, CreateSnapshotListener)} is called and makes sure that no snapshots is currently running
 * and registers the new snapshot in cluster state</li>
 * <li>When cluster state is updated the {@link SnapshotManager#beginSnapshot} method
 * kicks in and initializes the snapshot in the repository and then populates list of shards that needs to be snapshotted in cluster state</li>
 * <li>Each data node is watching for these shards and when new shards scheduled for snapshotting appear in the cluster state, data nodes
 * start processing them through {@link SnapshotsShardService#processIndexShardSnapshots} method</li>
 * <li>Once shard snapshot is created data node updates state of the shard in the cluster state using the {@link SnapshotsShardService#updateIndexShardSnapshotStatus} method</li>
 * <li>When last shard is completed master node in {@link SnapshotsShardService#innerUpdateSnapshotState} method marks the snapshot as completed</li>
 * <li>After cluster state is updated, the {@link SnapshotManager#endSnapshot(SnapshotsInProgress.Entry)} finalizes snapshot in the repository,
 * notifies all {@link SnapshotManager#snapshotCompletionListeners} that snapshot is completed, and finally calls {@link SnapshotManager#removeSnapshotFromClusterState}
 * to remove snapshot from cluster state</li>
 * </ul>
 */
public class SnapshotsService extends AbstractComponent {

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final SnapshotManager snapshotManager;

    @Inject
    public SnapshotsService(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService,
                            SnapshotManager snapshotManager) {
        super(settings);
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.snapshotManager = snapshotManager;
    }

    /**
     * Retrieves snapshot from repository
     *
     * @param snapshotId snapshot id
     * @return snapshot
     * @throws SnapshotMissingException if snapshot is not found
     */
    public Snapshot snapshot(SnapshotId snapshotId) {
        List<SnapshotsInProgress.Entry> entries = currentSnapshots(snapshotId.getRepository(), new String[]{snapshotId.getSnapshot()});
        if (!entries.isEmpty()) {
            return inProgressSnapshot(entries.iterator().next());
        }
        return repositoriesService.repository(snapshotId.getRepository()).readSnapshot(snapshotId);
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @return list of snapshots
     */
    public List<Snapshot> snapshots(String repositoryName) {
        Set<Snapshot> snapshotSet = newHashSet();
        List<SnapshotsInProgress.Entry> entries = currentSnapshots(repositoryName, null);
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotSet.add(inProgressSnapshot(entry));
        }
        Repository repository = repositoriesService.repository(repositoryName);
        List<SnapshotId> snapshotIds = repository.snapshots();
        for (SnapshotId snapshotId : snapshotIds) {
            snapshotSet.add(repository.readSnapshot(snapshotId));
        }
        ArrayList<Snapshot> snapshotList = newArrayList(snapshotSet);
        CollectionUtil.timSort(snapshotList);
        return ImmutableList.copyOf(snapshotList);
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @return list of snapshots
     */
    public List<Snapshot> currentSnapshots(String repositoryName) {
        List<Snapshot> snapshotList = newArrayList();
        List<SnapshotsInProgress.Entry> entries = currentSnapshots(repositoryName, null);
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(inProgressSnapshot(entry));
        }
        CollectionUtil.timSort(snapshotList);
        return ImmutableList.copyOf(snapshotList);
    }

    /**
     * Initializes the snapshotting process.
     * <p/>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     */
    public void createSnapshot(final SnapshotRequest request, final CreateSnapshotListener listener) {
        snapshotManager.createSnapshot(request, listener);
    }

    public void deleteSnapshot(final SnapshotId snapshotId, final SnapshotsService.DeleteSnapshotListener listener) {
        snapshotManager.deleteSnapshot(snapshotId, listener);
    }

    private Snapshot inProgressSnapshot(SnapshotsInProgress.Entry entry) {
        return new Snapshot(entry.snapshotId().getSnapshot(), entry.indices(), entry.startTime());
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param repository repository id
     * @param snapshots  optional list of snapshots that will be used as a filter
     * @return list of metadata for currently running snapshots
     */
    public List<SnapshotsInProgress.Entry> currentSnapshots(String repository, String[] snapshots) {
        SnapshotsInProgress snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
            return ImmutableList.of();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.entries();
        }
        if (snapshotsInProgress.entries().size() == 1) {
            // Most likely scenario - one snapshot is currently running
            // Check this snapshot against the query
            SnapshotsInProgress.Entry entry = snapshotsInProgress.entries().get(0);
            if (!entry.snapshotId().getRepository().equals(repository)) {
                return ImmutableList.of();
            }
            if (snapshots != null && snapshots.length > 0) {
                for (String snapshot : snapshots) {
                    if (entry.snapshotId().getSnapshot().equals(snapshot)) {
                        return snapshotsInProgress.entries();
                    }
                }
                return ImmutableList.of();
            } else {
                return snapshotsInProgress.entries();
            }
        }
        ImmutableList.Builder<SnapshotsInProgress.Entry> builder = ImmutableList.builder();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (!entry.snapshotId().getRepository().equals(repository)) {
                continue;
            }
            if (snapshots != null && snapshots.length > 0) {
                for (String snapshot : snapshots) {
                    if (entry.snapshotId().getSnapshot().equals(snapshot)) {
                        builder.add(entry);
                        break;
                    }
                }
            } else {
                builder.add(entry);
            }
        }
        return builder.build();
    }

    /**
     * Returns status of shards  currently finished snapshots
     * <p>
     * This method is executed on master node and it's complimentary to the {@link SnapshotsShardService#currentSnapshotShards(SnapshotId)} because it
     * returns simliar information but for already finished snapshots.
     * </p>
     *
     * @param snapshotId snapshot id
     * @return map of shard id to snapshot status
     */
    public ImmutableMap<ShardId, IndexShardSnapshotStatus> snapshotShards(SnapshotId snapshotId) throws IOException {
        ImmutableMap.Builder<ShardId, IndexShardSnapshotStatus> shardStatusBuilder = ImmutableMap.builder();
        Repository repository = repositoriesService.repository(snapshotId.getRepository());
        IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(snapshotId.getRepository());
        Snapshot snapshot = repository.readSnapshot(snapshotId);
        MetaData metaData = repository.readSnapshotMetaData(snapshotId, snapshot, snapshot.indices());
        for (String index : snapshot.indices()) {
            IndexMetaData indexMetaData = metaData.indices().get(index);
            if (indexMetaData != null) {
                int numberOfShards = indexMetaData.getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    ShardId shardId = new ShardId(index, i);
                    SnapshotShardFailure shardFailure = findShardFailure(snapshot.shardFailures(), shardId);
                    if (shardFailure != null) {
                        IndexShardSnapshotStatus shardSnapshotStatus = new IndexShardSnapshotStatus();
                        shardSnapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FAILURE);
                        shardSnapshotStatus.failure(shardFailure.reason());
                        shardStatusBuilder.put(shardId, shardSnapshotStatus);
                    } else {
                        IndexShardSnapshotStatus shardSnapshotStatus = indexShardRepository.snapshotStatus(snapshotId, shardId);
                        shardStatusBuilder.put(shardId, shardSnapshotStatus);
                    }
                }
            }
        }
        return shardStatusBuilder.build();
    }

    private SnapshotShardFailure findShardFailure(List<SnapshotShardFailure> shardFailures, ShardId shardId) {
        for (SnapshotShardFailure shardFailure : shardFailures) {
            if (shardId.getIndex().equals(shardFailure.index()) && shardId.getId() == shardFailure.shardId()) {
                return shardFailure;
            }
        }
        return null;
    }

    /**
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     */
    public static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE);
        if (snapshots != null) {
            for (SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                if (repository.equals(snapshot.snapshotId().getRepository())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Adds snapshot completion listener
     *
     * @param listener listener
     */
    public void addListener(SnapshotCompletionListener listener) {
        snapshotManager.addListener(listener);
    }

    /**
     * Removes snapshot completion listener
     *
     * @param listener listener
     */
    public void removeListener(SnapshotCompletionListener listener) {
        snapshotManager.removeListener(listener);
    }

    /**
     * Listener for create snapshot operation
     */
    public interface CreateSnapshotListener {

        /**
         * Called when snapshot has successfully started
         */
        void onResponse();

        /**
         * Called if a snapshot operation couldn't start
         */
        void onFailure(Throwable t);
    }

    /**
     * Listener for delete snapshot operation
     */
    public interface DeleteSnapshotListener {

        /**
         * Called if delete operation was successful
         */
        void onResponse();

        /**
         * Called if delete operation failed
         */
        void onFailure(Throwable t);
    }

    public interface SnapshotCompletionListener {

        void onSnapshotCompletion(SnapshotId snapshotId, SnapshotInfo snapshot);

        void onSnapshotFailure(SnapshotId snapshotId, Throwable t);
    }

    /**
     * Snapshot creation request
     */
    public static class SnapshotRequest {

        private String cause;

        private String name;

        private String repository;

        private String[] indices;

        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        private boolean partial;

        private Settings settings;

        private boolean includeGlobalState;

        private TimeValue masterNodeTimeout;

        /**
         * Constructs new snapshot creation request
         *
         * @param cause      cause for snapshot operation
         * @param name       name of the snapshot
         * @param repository name of the repository
         */
        public SnapshotRequest(String cause, String name, String repository) {
            this.cause = cause;
            this.name = name;
            this.repository = repository;
        }

        /**
         * Sets the list of indices to be snapshotted
         *
         * @param indices list of indices
         * @return this request
         */
        public SnapshotRequest indices(String[] indices) {
            this.indices = indices;
            return this;
        }

        /**
         * Sets repository-specific snapshot settings
         *
         * @param settings snapshot settings
         * @return this request
         */
        public SnapshotRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        /**
         * Set to true if global state should be stored as part of the snapshot
         *
         * @param includeGlobalState true if global state should be stored as part of the snapshot
         * @return this request
         */
        public SnapshotRequest includeGlobalState(boolean includeGlobalState) {
            this.includeGlobalState = includeGlobalState;
            return this;
        }

        /**
         * Sets master node timeout
         *
         * @param masterNodeTimeout master node timeout
         * @return this request
         */
        public SnapshotRequest masterNodeTimeout(TimeValue masterNodeTimeout) {
            this.masterNodeTimeout = masterNodeTimeout;
            return this;
        }

        /**
         * Sets the indices options
         *
         * @param indicesOptions indices options
         * @return this request
         */
        public SnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        /**
         * Set to true if partial snapshot should be allowed
         *
         * @param partial true if partial snapshots should be allowed
         * @return this request
         */
        public SnapshotRequest partial(boolean partial) {
            this.partial = partial;
            return this;
        }

        public boolean partial() {
            return partial;
        }

        /**
         * Returns cause for snapshot operation
         *
         * @return cause for snapshot operation
         */
        public String cause() {
            return cause;
        }

        /**
         * Returns snapshot name
         *
         * @return snapshot name
         */
        public String name() {
            return name;
        }

        /**
         * Returns snapshot repository
         *
         * @return snapshot repository
         */
        public String repository() {
            return repository;
        }

        /**
         * Returns the list of indices to be snapshotted
         *
         * @return the list of indices
         */
        public String[] indices() {
            return indices;
        }

        /**
         * Returns indices options
         *
         * @return indices options
         */
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        /**
         * Returns repository-specific settings for the snapshot operation
         *
         * @return repository-specific settings
         */
        public Settings settings() {
            return settings;
        }

        /**
         * Returns true if global state should be stored as part of the snapshot
         *
         * @return true if global state should be stored as part of the snapshot
         */
        public boolean includeGlobalState() {
            return includeGlobalState;
        }

        /**
         * Returns master node timeout
         *
         * @return master node timeout
         */
        public TimeValue masterNodeTimeout() {
            return masterNodeTimeout;
        }

    }
}

