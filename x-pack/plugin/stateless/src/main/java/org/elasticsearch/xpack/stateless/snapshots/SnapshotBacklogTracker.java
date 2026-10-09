/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Runs on index nodes and tracks, per repository, how many bytes of the primary shards on this node the repository does not have yet,
 * i.e. what the next snapshot into the repository will have to upload. Unlike the counters of a running snapshot, this is known before
 * the snapshot starts, so there is time to act on it.
 * <p>
 * For one shard the backlog is the total length of the files of its current commit that the repository does not hold, see
 * {@link ShardBacklog}, minus what a running snapshot of the shard has already uploaded. What the repository holds comes from the
 * shard's latest shard-level metadata, which {@link RepositoryFilesCache} keeps current. A shard whose repository files are not known yet
 * (this node just started, or the shard just arrived, or a snapshot just finished or got deleted) is not counted, but reported as an
 * unknown shard so that it is never mistaken for a shard with nothing to upload.
 * <p>
 * Repositories that are read-only are not tracked. Every other registered repository is, whether or not a snapshot of the node's shards
 * is going to target it.
 */
public class SnapshotBacklogTracker implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotBacklogTracker.class);

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
     * How many reads of repository metadata (the repository data and the shard-level metadata of shards) may run at the same time on
     * this node. When a node starts with thousands of shards, every one of them needs one read, and the repository's object store must
     * not be flooded with them. The reads are small, so a handful at a time still finishes within a few evaluation intervals.
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
     * A primary shard on this node, with the files of its current commit, or {@code null} if they cannot be determined at the moment.
     */
    record LocalShard(ShardId shardId, ProjectId projectId, @Nullable Map<String, BlobLocation> commitFiles) {}

    private record TrackedRepository(BlobStoreRepository repository, RepositoryFilesCache cache) {}

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final StatelessCommitService commitService;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final Executor repositoryReadExecutor;

    private final Map<ProjectRepo, TrackedRepository> trackedRepositories = new ConcurrentHashMap<>();
    // The statuses of the shard snapshots running on this node, to take the progress of an upload into account
    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> runningShardSnapshots = new ConcurrentHashMap<>();

    // The result of the latest periodic evaluation, which the metrics report
    private volatile Map<ProjectRepo, RepositoryBacklog> latestBacklog = Map.of();
    private volatile Scheduler.Cancellable evaluationTask;

    public SnapshotBacklogTracker(
        ClusterService clusterService,
        IndicesService indicesService,
        StatelessCommitService commitService,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.commitService = commitService;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        // Repository metadata is read on the snapshot_meta pool, which is meant for that and is allowed to do repository I/O.
        this.repositoryReadExecutor = new ThrottledTaskRunner(
            "snapshot-backlog-repository-reads",
            MAX_CONCURRENT_REPOSITORY_READS,
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
        ).asExecutor();
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
        runningShardSnapshots.computeIfAbsent(snapshot, s -> new ConcurrentHashMap<>()).put(shardId, status);
    }

    private void evaluate() {
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
        updateTrackedRepositories();
        if (trackedRepositories.isEmpty()) {
            return Map.of();
        }
        final List<LocalShard> localShards = getLocalShards();
        final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
        runningShardSnapshots.keySet().removeIf(snapshot -> snapshotsInProgress.snapshot(snapshot) == null);

        final Map<ProjectRepo, RepositoryBacklog> backlogs = new HashMap<>();
        trackedRepositories.forEach((projectRepo, tracked) -> {
            tracked.cache().onRepositoryGeneration(tracked.repository().getMetadata().generation());
            final var shardsOfProject = localShards.stream().filter(shard -> shard.projectId().equals(projectRepo.projectId())).toList();
            tracked.cache().retainShards(shardsOfProject.stream().map(LocalShard::shardId).collect(Collectors.toSet()));
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
                        : new TrackedRepository(
                            blobStoreRepository,
                            new RepositoryFilesCache(projectRepo.name(), newReader(blobStoreRepository), repositoryReadExecutor)
                        )
                );
            }
        }
        trackedRepositories.keySet().retainAll(current);
    }

    private static RepositoryFilesCache.Reader newReader(BlobStoreRepository repository) {
        return new RepositoryFilesCache.Reader() {
            @Override
            public RepositoryData readRepositoryData(long repositoryGeneration) throws IOException {
                return repository.readRepositoryData(repositoryGeneration);
            }

            @Override
            public BlobStoreIndexShardSnapshots readShardSnapshots(IndexId indexId, int shardId, ShardGeneration shardGeneration)
                throws IOException {
                return repository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGeneration);
            }
        };
    }

    private List<LocalShard> getLocalShards() {
        final var metadata = clusterService.state().metadata();
        final List<LocalShard> shards = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard.routingEntry().primary() == false || shard.state() != IndexShardState.STARTED) {
                    continue;
                }
                final Optional<ProjectMetadata> project = metadata.lookupProject(shard.shardId().getIndex());
                if (project.isPresent()) {
                    shards.add(new LocalShard(shard.shardId(), project.get().id(), getCommitFiles(shard)));
                }
            }
        }
        return shards;
    }

    /**
     * @return the files of the latest commit of the shard with their locations (the lengths of the files), without flushing the shard
     *         as a snapshot would, or {@code null} if they are not available at the moment.
     */
    @Nullable
    private Map<String, BlobLocation> getCommitFiles(IndexShard shard) {
        try (var commitRef = shard.acquireLastIndexCommit(false)) {
            final Map<String, BlobLocation> commitFiles = new HashMap<>();
            for (String fileName : commitRef.getIndexCommit().getFileNames()) {
                final BlobLocation blobLocation = commitService.getBlobLocation(shard.shardId(), fileName);
                if (blobLocation == null) {
                    return null;
                }
                commitFiles.put(fileName, blobLocation);
            }
            return commitFiles;
        } catch (Exception e) {
            // e.g. the shard is closing
            logger.debug(() -> "cannot get the commit files of " + shard.shardId(), e);
            return null;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // pick up a new repository generation (a snapshot finished, or was deleted) without waiting for the next evaluation
        trackedRepositories.values()
            .forEach(tracked -> tracked.cache().onRepositoryGeneration(tracked.repository().getMetadata().generation()));
    }
}
