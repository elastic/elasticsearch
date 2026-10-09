/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.ShardGenerationsRefresher.Trigger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Runs on index nodes and tracks, per repository, how many bytes of the primary shards on this node the repository does not have yet,
 * i.e. what the next snapshot into the repository will have to upload. Unlike the counters of a running snapshot, this is known before
 * the snapshot starts, so there is time to act on it.
 * <p>
 * For one shard the backlog is the total length of the files of its latest local commit that the repository does not hold, see
 * {@link ShardBacklog}, minus what a running snapshot of the shard has already uploaded. It is a lower bound of what a snapshot started
 * now would upload, because a snapshot flushes the shard first and so captures a commit that is newer than the latest one. What the
 * repository holds comes from the shard's latest shard-level metadata, which {@link RepositoryFilesCache} keeps current. The generation of
 * that metadata comes from the master, which this node asks whenever the repository generation in the cluster state changes or a shard
 * starts on the node (see {@link ShardGenerationsRefresher}), so this node never reads the root blob of the repository. A shard whose
 * repository files are not known yet (this node just started, or the shard just arrived, or a snapshot just finished or got deleted) is
 * not counted, but reported as an unknown shard so that it is never mistaken for a shard with nothing to upload.
 * <p>
 * Repositories that are read-only are not tracked. Every other registered repository is, whether or not a snapshot of the node's shards
 * is going to target it. Nothing is tracked, requested, read, reported or logged unless {@link #BACKLOG_TRACKING_ENABLED_SETTING} is on.
 */
public class SnapshotBacklogTracker implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotBacklogTracker.class);

    public static final Setting<Boolean> BACKLOG_TRACKING_ENABLED_SETTING = Setting.boolSetting(
        "stateless.snapshot.backlog_tracking.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // The metric names end in a suffix that the APM metric validator accepts, the attribute in the namespaced form it asks for.
    static final String BACKLOG_BYTES_METRIC = "es.repositories.snapshots.backlog.bytes.current";
    static final String BACKLOG_UNKNOWN_SHARDS_METRIC = "es.repositories.snapshots.backlog.unknown_shards.current";
    static final String REPOSITORY_NAME_ATTRIBUTE = "es_repository_name";

    /**
     * How often the backlog is evaluated, logged and made available to the metrics. A snapshot into a repository normally takes minutes,
     * so a more frequent evaluation would add cost without adding information.
     */
    static final TimeValue EVALUATION_INTERVAL = TimeValue.timeValueSeconds(30);

    /**
     * How many reads of repository metadata (the shard-level metadata of shards) may run at the same time on this node. When a node
     * starts with thousands of shards, every one of them needs one read, and the repository's object store must not be flooded with them.
     * The reads are small, so a handful at a time still finishes within a few evaluation intervals.
     */
    static final int MAX_CONCURRENT_REPOSITORY_READS = 4;

    /**
     * The backlog of one repository on this node.
     *
     * @param bytes             the total backlog of the shards that are counted
     * @param countedShards     the number of shards whose backlog is known and included in {@code bytes}
     * @param unknownShards     the number of shards whose backlog is not known yet, and therefore not included in {@code bytes}
     * @param largestShardBytes the largest backlog of a single shard
     */
    public record RepositoryBacklog(long bytes, int countedShards, int unknownShards, long largestShardBytes) {

        public boolean isEmpty() {
            return bytes == 0 && unknownShards == 0;
        }
    }

    /**
     * A primary shard on this node, with the names and lengths of the files of its latest commit, or {@code null} if they cannot be
     * determined at the moment.
     */
    record LocalShard(ShardId shardId, ProjectId projectId, @Nullable Map<String, Long> commitFiles) {}

    private record TrackedRepository(BlobStoreRepository repository, RepositoryFilesCache cache, ShardGenerationsRefresher refresher) {

        void close() {
            refresher.close();
            cache.close();
        }
    }

    private final ClusterService clusterService;
    private final Client client;
    private final IndicesService indicesService;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final Executor repositoryReadExecutor;

    private final Map<ProjectRepo, TrackedRepository> trackedRepositories = new ConcurrentHashMap<>();
    // The statuses of the shard snapshots running on this node, to take the progress of an upload into account
    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> runningShardSnapshots = new ConcurrentHashMap<>();

    // The result of the latest periodic evaluation, which the metrics report
    private volatile Map<ProjectRepo, RepositoryBacklog> latestBacklog = Map.of();
    private volatile Scheduler.Cancellable evaluationTask;
    private volatile boolean enabled;

    public SnapshotBacklogTracker(
        ClusterService clusterService,
        Client client,
        IndicesService indicesService,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.indicesService = indicesService;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        // Shard-level metadata is read on the snapshot_meta pool, which is meant for that and is allowed to do repository I/O.
        this.repositoryReadExecutor = new ThrottledTaskRunner(
            "snapshot-backlog-repository-reads",
            MAX_CONCURRENT_REPOSITORY_READS,
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
        ).asExecutor();
        clusterService.getClusterSettings().initializeAndWatch(BACKLOG_TRACKING_ENABLED_SETTING, this::setEnabled);
        meterRegistry.registerLongAsyncGauge(
            BACKLOG_BYTES_METRIC,
            "Bytes of this node's primary shards that a snapshot into the repository has yet to upload",
            "bytes",
            measurement -> latestBacklog.forEach(
                (repo, backlog) -> measurement.record(backlog.bytes(), Map.of(REPOSITORY_NAME_ATTRIBUTE, repo.name()))
            )
        );
        meterRegistry.registerLongAsyncGauge(
            BACKLOG_UNKNOWN_SHARDS_METRIC,
            "Primary shards on this node whose snapshot backlog for the repository is not known yet",
            "unit",
            measurement -> latestBacklog.forEach(
                (repo, backlog) -> measurement.record(backlog.unknownShards(), Map.of(REPOSITORY_NAME_ATTRIBUTE, repo.name()))
            )
        );
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled == false) {
            // forget everything, so that nothing is reported or kept, and tracking starts from scratch if it is turned on again
            latestBacklog = Map.of();
            runningShardSnapshots.clear();
            trackedRepositories.values().forEach(TrackedRepository::close);
            trackedRepositories.clear();
        }
    }

    /**
     * Starts evaluating the backlog periodically.
     */
    public void start() {
        evaluationTask = threadPool.scheduleWithFixedDelay(this::evaluate, EVALUATION_INTERVAL, threadPool.generic());
    }

    public void stop() {
        final var task = evaluationTask;
        if (task != null) {
            task.cancel();
        }
    }

    /**
     * Makes the tracker aware of a shard snapshot that is running on this node, so that the data it has uploaded so far is taken off the
     * backlog of the shard.
     */
    public void registerShardSnapshot(Snapshot snapshot, ShardId shardId, IndexShardSnapshotStatus status) {
        if (enabled == false) {
            return;
        }
        runningShardSnapshots.computeIfAbsent(snapshot, s -> new ConcurrentHashMap<>()).put(shardId, status);
    }

    private void evaluate() {
        if (enabled == false) {
            return;
        }
        try {
            final var backlog = getBacklog();
            latestBacklog = backlog;
            backlog.forEach((repo, repoBacklog) -> {
                if (repoBacklog.isEmpty() == false) {
                    logger.info(
                        "snapshot backlog of repository [{}]: {} bytes in {} shards, {} shards unknown, largest shard backlog {} bytes",
                        repo.name(),
                        repoBacklog.bytes(),
                        repoBacklog.countedShards(),
                        repoBacklog.unknownShards(),
                        repoBacklog.largestShardBytes()
                    );
                }
            });
        } catch (Exception e) {
            logger.warn("failed to evaluate the snapshot backlog", e);
        }
    }

    /**
     * Computes the current backlog of every tracked repository from what is cached, and starts reading what is missing from the
     * repositories. It does not wait for those reads: the shards they are for are reported as unknown until they are done.
     */
    public Map<ProjectRepo, RepositoryBacklog> getBacklog() {
        if (enabled == false) {
            return Map.of();
        }
        updateTrackedRepositories();
        if (trackedRepositories.isEmpty()) {
            return Map.of();
        }
        final List<LocalShard> localShards = getLocalShards();
        final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
        runningShardSnapshots.keySet().removeIf(snapshot -> snapshotsInProgress.snapshot(snapshot) == null);

        final Map<ProjectRepo, RepositoryBacklog> backlogs = new HashMap<>();
        trackedRepositories.forEach((projectRepo, tracked) -> {
            final var shardsOfProject = localShards.stream().filter(shard -> shard.projectId().equals(projectRepo.projectId())).toList();
            final Set<ShardId> shardIdsOfProject = shardsOfProject.stream().map(LocalShard::shardId).collect(Collectors.toSet());
            tracked.cache().retainShards(shardIdsOfProject);
            tracked.refresher().refresh(Trigger.TICK, tracked.repository().getMetadata().generation(), shardIdsOfProject);
            backlogs.put(
                projectRepo,
                computeRepositoryBacklog(tracked.cache(), shardsOfProject, shardId -> getRunningShardSnapshots(projectRepo, shardId))
            );
        });
        return Map.copyOf(backlogs);
    }

    static RepositoryBacklog computeRepositoryBacklog(
        RepositoryFilesCache cache,
        Collection<LocalShard> shards,
        Function<ShardId, Collection<IndexShardSnapshotStatus>> runningSnapshots
    ) {
        long bytes = 0;
        int counted = 0;
        int unknown = 0;
        long largest = 0;
        for (LocalShard shard : shards) {
            // always ask the cache, so that it starts reading what it lacks, even if the commit files are not available
            final var repositoryFiles = cache.getShardFiles(shard.shardId());
            if (repositoryFiles == null || shard.commitFiles() == null) {
                unknown++;
                continue;
            }
            final var shardBacklog = ShardBacklog.of(shard.commitFiles(), repositoryFiles)
                .minusRunningSnapshots(repositoryFiles, runningSnapshots.apply(shard.shardId()));
            bytes += shardBacklog.bytes();
            counted++;
            largest = Math.max(largest, shardBacklog.bytes());
        }
        return new RepositoryBacklog(bytes, counted, unknown, largest);
    }

    private List<IndexShardSnapshotStatus> getRunningShardSnapshots(ProjectRepo projectRepo, ShardId shardId) {
        final List<IndexShardSnapshotStatus> statuses = new ArrayList<>();
        runningShardSnapshots.forEach((snapshot, shards) -> {
            if (snapshot.getProjectId().equals(projectRepo.projectId()) && snapshot.getRepository().equals(projectRepo.name())) {
                final var status = shards.get(shardId);
                if (status != null) {
                    statuses.add(status);
                }
            }
        });
        return statuses;
    }

    private void updateTrackedRepositories() {
        final Set<ProjectRepo> current = new HashSet<>();
        for (Repository repository : repositoriesService.getRepositories()) {
            if (repository instanceof BlobStoreRepository blobStoreRepository && blobStoreRepository.isReadOnly() == false) {
                final var projectRepo = blobStoreRepository.getProjectRepo();
                current.add(projectRepo);
                // a repository whose settings changed is a new instance, and may well point somewhere else
                trackedRepositories.compute(
                    projectRepo,
                    (key, tracked) -> tracked != null && tracked.repository() == blobStoreRepository
                        ? tracked
                        : newTrackedRepository(projectRepo, blobStoreRepository)
                );
            }
        }
        trackedRepositories.entrySet().removeIf(entry -> {
            if (current.contains(entry.getKey())) {
                return false;
            }
            entry.getValue().close();
            return true;
        });
    }

    private TrackedRepository newTrackedRepository(ProjectRepo projectRepo, BlobStoreRepository repository) {
        final var cache = new RepositoryFilesCache(projectRepo.name(), newReader(repository), repositoryReadExecutor);
        final var refresher = new ShardGenerationsRefresher(
            projectRepo.name(),
            (observedGeneration, shardIds, listener) -> getShardGenerationsFromMaster(projectRepo, observedGeneration, shardIds, listener),
            cache
        );
        return new TrackedRepository(repository, cache, refresher);
    }

    private void getShardGenerationsFromMaster(
        ProjectRepo projectRepo,
        long observedGeneration,
        List<ShardId> shardIds,
        ActionListener<GetShardGenerationsResponse> listener
    ) {
        // This is background work of the node, not of whoever's request or cluster state change happens to trigger it
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            client.execute(
                TransportGetShardGenerationsAction.TYPE,
                // a request that waits for a master for longer than an evaluation is not wanted: the next evaluation asks again
                new GetShardGenerationsRequest(EVALUATION_INTERVAL, projectRepo, observedGeneration, shardIds),
                listener
            );
        }
    }

    private static RepositoryFilesCache.Reader newReader(BlobStoreRepository repository) {
        return repository::getBlobStoreIndexShardSnapshots;
    }

    private List<LocalShard> getLocalShards() {
        final List<LocalShard> shards = new ArrayList<>();
        forEachStartedPrimary((shard, projectId) -> shards.add(new LocalShard(shard.shardId(), projectId, getCommitFiles(shard))));
        return shards;
    }

    private void forEachStartedPrimary(BiConsumer<IndexShard, ProjectId> consumer) {
        final var metadata = clusterService.state().metadata();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard.routingEntry().primary() == false || shard.state() != IndexShardState.STARTED) {
                    continue;
                }
                final Optional<ProjectMetadata> project = metadata.lookupProject(shard.shardId().getIndex());
                project.ifPresent(projectMetadata -> consumer.accept(shard, projectMetadata.id()));
            }
        }
    }

    /**
     * @return the names and lengths of the files of the latest commit of the shard, without flushing the shard as a snapshot would, or
     *         {@code null} if they are not available at the moment.
     */
    @Nullable
    private Map<String, Long> getCommitFiles(IndexShard shard) {
        try (var commitRef = shard.acquireLastIndexCommit(false)) {
            return getCommitFiles(commitRef.getIndexCommit());
        } catch (Exception e) {
            // e.g. the shard is closing
            logger.debug(() -> "cannot get the commit files of " + shard.shardId(), e);
            return null;
        }
    }

    /**
     * The files of a commit with their lengths, which the shard knows locally. That includes the files of a commit that is not uploaded
     * yet, which is normal for a shard that is being indexed into, and exactly what makes up the backlog. The length of a file comes from
     * the directory of the commit, which does not read the file's contents.
     */
    static Map<String, Long> getCommitFiles(IndexCommit commit) throws IOException {
        final Map<String, Long> commitFiles = new HashMap<>();
        for (String fileName : commit.getFileNames()) {
            commitFiles.put(fileName, commit.getDirectory().fileLength(fileName));
        }
        return commitFiles;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (enabled == false || trackedRepositories.isEmpty()) {
            return;
        }
        // Pick up a new repository generation (a snapshot finished, or was deleted), and the shards that started on this node, without
        // waiting for the next evaluation. Looking at the shards is only worth it if one of those may have happened.
        final boolean repositoryGenerationChanged = trackedRepositories.values()
            .stream()
            .anyMatch(tracked -> tracked.repository().getMetadata().generation() > tracked.cache().getRepositoryGeneration());
        if (repositoryGenerationChanged == false && event.routingTableChanged() == false) {
            return;
        }
        final Map<ProjectId, Set<ShardId>> shardsByProject = new HashMap<>();
        forEachStartedPrimary((shard, projectId) -> shardsByProject.computeIfAbsent(projectId, id -> new HashSet<>()).add(shard.shardId()));
        trackedRepositories.forEach(
            (projectRepo, tracked) -> tracked.refresher()
                .refresh(
                    Trigger.CLUSTER_STATE,
                    tracked.repository().getMetadata().generation(),
                    shardsByProject.getOrDefault(projectRepo.projectId(), Set.of())
                )
        );
    }
}
