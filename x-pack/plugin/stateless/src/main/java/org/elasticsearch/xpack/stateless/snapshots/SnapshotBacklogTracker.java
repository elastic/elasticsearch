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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
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
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.ShardGenerationsRefresher.Trigger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>
 * All the state of the tracker is changed on one task queue, one task at a time: the periodic evaluation, the processing of a cluster
 * state change, what is done with an answer of the master or with a read of the repository, and turning the tracking off. What the
 * evaluation computes is published as one immutable result, which {@link #getBacklog()} and the metrics only read.
 */
public class SnapshotBacklogTracker implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotBacklogTracker.class);

    public static final Setting<Boolean> BACKLOG_TRACKING_ENABLED_SETTING = Setting.boolSetting(
        "stateless.snapshot.backlog_tracking.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * How often the backlog is evaluated, logged and made available to the metrics. A snapshot into a repository normally takes minutes,
     * so a more frequent evaluation would add cost without adding information. Tests make it shorter.
     */
    public static final Setting<TimeValue> EVALUATION_INTERVAL_SETTING = Setting.timeSetting(
        "stateless.snapshot.backlog_tracking.evaluation_interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    // The metric names end in a suffix that the APM metric validator accepts, the attribute in the namespaced form it asks for.
    static final String BACKLOG_BYTES_METRIC = "es.repositories.snapshots.backlog.bytes.current";
    static final String BACKLOG_UNKNOWN_SHARDS_METRIC = "es.repositories.snapshots.backlog.unknown_shards.current";
    static final String REPOSITORY_NAME_ATTRIBUTE = "es_repository_name";

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
     * @param inlinedBytes      the total length of the files of the counted shards that a snapshot would not upload as data blobs but
     *                          keep inside the shard-level metadata, see {@link ShardBacklog}; not part of {@code bytes}
     */
    public record RepositoryBacklog(long bytes, int countedShards, int unknownShards, long largestShardBytes, long inlinedBytes) {

        public boolean isEmpty() {
            return bytes == 0 && unknownShards == 0;
        }
    }

    /**
     * The backlog of one shard, for the debug log of what a repository's backlog is made of.
     *
     * @param known        whether the backlog of the shard is known; if not the other values are zero
     * @param bytes        see {@link ShardBacklog}
     * @param inlinedBytes see {@link ShardBacklog}
     */
    record ShardDetail(ShardId shardId, boolean known, long bytes, long inlinedBytes) {}

    /**
     * A primary shard on this node, with the names and lengths of the files of its latest commit, or {@code null} if they cannot be
     * determined at the moment.
     */
    record LocalShard(ShardId shardId, ProjectId projectId, @Nullable Map<String, Long> commitFiles) {}

    /**
     * The started primary shards on this node
     */
    interface LocalShards {
        /**
         * @return the shards with their commit files, which is more work than {@link #getShardIds()}
         */
        List<LocalShard> getShards();

        /**
         * @return the ids of the shards of each project
         */
        Map<ProjectId, Set<ShardId>> getShardIds();
    }

    private record TrackedRepository(BlobStoreRepository repository, RepositoryFilesCache cache, ShardGenerationsRefresher refresher) {

        void close() {
            refresher.close();
            cache.close();
        }
    }

    private final ClusterService clusterService;
    private final Client client;
    private final LocalShards localShards;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final TimeValue evaluationInterval;
    private final Executor repositoryReadExecutor;
    // Runs everything that changes the state of the tracker, one task at a time
    private final Executor stateExecutor;
    private final AtomicBoolean evaluationQueued = new AtomicBoolean();
    private final AtomicBoolean clusterStateRefreshQueued = new AtomicBoolean();

    // The state of the tracker, only used on the state executor
    private final Map<ProjectRepo, TrackedRepository> trackedRepositories = new HashMap<>();

    // The statuses of the shard snapshots on this node, to take what they upload into account until the repository files of the shard are
    // known to include it. Shard snapshots add to it from their own threads, and the state executor takes the ones that are no longer
    // needed off, see pruneRunningShardSnapshots.
    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> runningShardSnapshots = new ConcurrentHashMap<>();
    // For a status that is done, the generation of the repository files of its shard when it was first seen as done
    private final Map<IndexShardSnapshotStatus, ShardGeneration> doneSnapshotsSeenAt = new HashMap<>();
    // The generation of the shard that each shard snapshot started from, which the status forgets when it is done. Added to from the
    // threads of the shard snapshots, and cleaned by the state executor like runningShardSnapshots.
    private final Map<IndexShardSnapshotStatus, ShardGeneration> startedFromGenerations = new ConcurrentHashMap<>();

    // The backlog of each shard in the latest evaluation, only used on the state executor
    private final Map<ProjectRepo, List<ShardDetail>> shardDetails = new HashMap<>();

    // The result of the latest evaluation, which is all that the metrics and getBacklog() read, or null if there is none
    @Nullable
    private volatile Map<ProjectRepo, RepositoryBacklog> latestBacklog;
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
        this(
            clusterService,
            client,
            new IndicesLocalShards(clusterService, indicesService),
            repositoriesService,
            threadPool,
            meterRegistry
        );
    }

    SnapshotBacklogTracker(
        ClusterService clusterService,
        Client client,
        LocalShards localShards,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.localShards = localShards;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.evaluationInterval = EVALUATION_INTERVAL_SETTING.get(clusterService.getSettings());
        this.stateExecutor = new ThrottledTaskRunner("snapshot-backlog-state", 1, threadPool.generic()).asExecutor();
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
            measurement -> getBacklog().forEach(
                (repo, backlog) -> measurement.record(backlog.bytes(), Map.of(REPOSITORY_NAME_ATTRIBUTE, repo.name()))
            )
        );
        meterRegistry.registerLongAsyncGauge(
            BACKLOG_UNKNOWN_SHARDS_METRIC,
            "Primary shards on this node whose snapshot backlog for the repository is not known yet",
            "unit",
            measurement -> getBacklog().forEach(
                (repo, backlog) -> measurement.record(backlog.unknownShards(), Map.of(REPOSITORY_NAME_ATTRIBUTE, repo.name()))
            )
        );
    }

    private void setEnabled(boolean enabled) {
        final boolean wasEnabled = this.enabled;
        this.enabled = enabled;
        if (wasEnabled && enabled == false) {
            // Forget everything, so that nothing is reported or kept, and tracking starts from scratch if it is turned on again. This
            // comes after whatever is running at the moment, which then does not publish its result (see evaluate).
            stateExecutor.execute(this::clear);
        }
    }

    private void clear() {
        latestBacklog = null;
        runningShardSnapshots.clear();
        doneSnapshotsSeenAt.clear();
        startedFromGenerations.clear();
        shardDetails.clear();
        trackedRepositories.values().forEach(TrackedRepository::close);
        trackedRepositories.clear();
    }

    /**
     * Starts evaluating the backlog periodically.
     */
    public void start() {
        evaluationTask = threadPool.scheduleWithFixedDelay(this::scheduleEvaluation, evaluationInterval, threadPool.generic());
    }

    public void stop() {
        final var task = evaluationTask;
        if (task != null) {
            task.cancel();
        }
    }

    // an evaluation that takes longer than the interval must not pile up more of them
    private void scheduleEvaluation() {
        if (enabled && evaluationQueued.compareAndSet(false, true)) {
            stateExecutor.execute(() -> {
                evaluationQueued.set(false);
                evaluate();
            });
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
        final ShardGeneration startedFrom = status.generation();
        if (startedFrom != null) {
            startedFromGenerations.put(status, startedFrom);
        }
        runningShardSnapshots.compute(snapshot, (key, shards) -> {
            final Map<ShardId, IndexShardSnapshotStatus> updated = shards == null ? new ConcurrentHashMap<>() : shards;
            updated.put(shardId, status);
            return updated;
        });
    }

    /**
     * Computes the backlog of every tracked repository from what is cached, publishes it, and starts reading what is missing from the
     * repositories. It does not wait for those reads: the shards they are for are reported as unknown until they are done.
     */
    private void evaluate() {
        if (enabled == false) {
            return;
        }
        try {
            updateTrackedRepositories();
            final Map<ProjectRepo, RepositoryBacklog> backlog = trackedRepositories.isEmpty() ? Map.of() : computeBacklog();
            if (enabled == false) {
                return; // turned off while evaluating: the task that clears everything is next, and nothing is published after it
            }
            latestBacklog = backlog;
            backlog.forEach((repo, repoBacklog) -> {
                if (repoBacklog.isEmpty() == false) {
                    logger.info(
                        "snapshot backlog of repository [{}]: {} bytes in {} shards, {} shards unknown, largest shard backlog {} bytes, "
                            + "{} bytes inlined in shard metadata",
                        repo.name(),
                        repoBacklog.bytes(),
                        repoBacklog.countedShards(),
                        repoBacklog.unknownShards(),
                        repoBacklog.largestShardBytes(),
                        repoBacklog.inlinedBytes()
                    );
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "snapshot backlog of repository [{}] per shard: {}",
                            repo.name(),
                            formatShardDetails(shardDetails.getOrDefault(repo, List.of()))
                        );
                    }
                }
            });
        } catch (Exception e) {
            logger.warn("failed to evaluate the snapshot backlog", e);
        }
    }

    private Map<ProjectRepo, RepositoryBacklog> computeBacklog() {
        final List<LocalShard> shards = localShards.getShards();
        final ClusterState state = clusterService.state();

        final Map<ProjectRepo, RepositoryBacklog> backlogs = new HashMap<>();
        shardDetails.clear();
        trackedRepositories.forEach((projectRepo, tracked) -> {
            final var shardsOfProject = shards.stream().filter(shard -> shard.projectId().equals(projectRepo.projectId())).toList();
            final Set<ShardId> shardIdsOfProject = shardsOfProject.stream().map(LocalShard::shardId).collect(Collectors.toSet());
            tracked.cache().retainShards(shardIdsOfProject);
            tracked.refresher().refresh(Trigger.TICK, tracked.repository().getMetadata().generation(), shardIdsOfProject);
            // first, so that what is no longer needed is not taken off the backlog of this evaluation already
            pruneRunningShardSnapshots(projectRepo, tracked, shardIdsOfProject, state);
            final List<ShardDetail> details = new ArrayList<>();
            shardDetails.put(projectRepo, details);
            backlogs.put(
                projectRepo,
                computeRepositoryBacklog(
                    tracked.cache(),
                    shardsOfProject,
                    shardId -> getRunningShardSnapshots(projectRepo, shardId),
                    details
                )
            );
        });
        retainStatusesOfRunningShardSnapshots();
        return Map.copyOf(backlogs);
    }

    // package-private for tests, which look at it when the state executor is idle
    int getRunningShardSnapshotCount() {
        return runningShardSnapshots.values().stream().mapToInt(Map::size).sum();
    }

    /**
     * @return the backlog of every tracked repository as of the latest evaluation, which is empty if there is none, e.g. because the
     *         tracking is off
     */
    public final Map<ProjectRepo, RepositoryBacklog> getBacklog() {
        final var backlog = latestBacklog;
        return backlog == null ? Map.of() : backlog;
    }

    static RepositoryBacklog computeRepositoryBacklog(
        RepositoryFilesCache cache,
        Collection<LocalShard> shards,
        Function<ShardId, Collection<IndexShardSnapshotStatus>> runningSnapshots
    ) {
        return computeRepositoryBacklog(cache, shards, runningSnapshots, new ArrayList<>());
    }

    /**
     * @param details where the backlog of each shard is added to
     */
    static RepositoryBacklog computeRepositoryBacklog(
        RepositoryFilesCache cache,
        Collection<LocalShard> shards,
        Function<ShardId, Collection<IndexShardSnapshotStatus>> runningSnapshots,
        List<ShardDetail> details
    ) {
        long bytes = 0;
        int counted = 0;
        int unknown = 0;
        long largest = 0;
        long inlined = 0;
        for (LocalShard shard : shards) {
            // always ask the cache, so that it starts reading what it lacks, even if the commit files are not available
            final var repositoryFiles = cache.getShardFiles(shard.shardId());
            if (repositoryFiles == null || shard.commitFiles() == null) {
                unknown++;
                details.add(new ShardDetail(shard.shardId(), false, 0L, 0L));
                continue;
            }
            final var shardBacklog = ShardBacklog.of(shard.commitFiles(), repositoryFiles)
                .minusRunningSnapshots(repositoryFiles, runningSnapshots.apply(shard.shardId()));
            bytes += shardBacklog.bytes();
            inlined += shardBacklog.inlinedBytes();
            details.add(new ShardDetail(shard.shardId(), true, shardBacklog.bytes(), shardBacklog.inlinedBytes()));
            counted++;
            largest = Math.max(largest, shardBacklog.bytes());
        }
        return new RepositoryBacklog(bytes, counted, unknown, largest, inlined);
    }

    /**
     * One compact line for the shards of a repository: {@code index[shard]=bytes} or {@code index[shard]=bytes+inlinedBytes} if the
     * shard has inlined bytes, and {@code index[shard]=unknown} if its backlog is not known yet, in the order of the index name and shard.
     */
    static String formatShardDetails(Collection<ShardDetail> details) {
        return details.stream()
            .sorted(Comparator.comparing((ShardDetail detail) -> detail.shardId().getIndexName()).thenComparingInt(d -> d.shardId().id()))
            .map(detail -> {
                final String value = detail.known() == false ? "unknown"
                    : detail.inlinedBytes() == 0 ? Long.toString(detail.bytes())
                    : detail.bytes() + "+" + detail.inlinedBytes();
                return detail.shardId().getIndexName() + "[" + detail.shardId().id() + "]=" + value;
            })
            .collect(Collectors.joining(" "));
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

    /**
     * Forgets the shard snapshots whose uploads do not have to be taken off the backlog any more. A snapshot that is done has uploaded
     * files that the repository files of the shard, which come from the master and a read, do not include until they have caught up.
     * Until then the snapshot stays, so that the backlog goes on being what is new since the snapshot, instead of jumping back to
     * everything that the shard has until the repository files are known to be up to date. That is when their generation is the one the
     * snapshot made. They can also move on to a newer one, if another snapshot or a deletion came first, which is just as good.
     * <p>
     * But a snapshot that is done is only worth waiting for while it can still finalize. When it cannot, because the snapshot failed or
     * was deleted, nothing in the repository refers to what it uploaded, and it is forgotten as soon as that is certain: the snapshot is
     * gone from the cluster state, and the repository files of the shard are up to date with a repository generation at least as new as
     * the one in that cluster state, so that they would include the snapshot if it had finalized. It is also forgotten when a newer
     * snapshot of the same shard has started from another generation than the one it made: that one uploads again what the older one did,
     * and the backlog would be understated if both counted. A newer one that started from the generation the older one made, as one that
     * was queued behind it does, does not upload those files again, so the older one stays until it is in the repository files.
     */
    private void pruneRunningShardSnapshots(
        ProjectRepo projectRepo,
        TrackedRepository tracked,
        Set<ShardId> localShards,
        ClusterState state
    ) {
        final var snapshotsInProgress = SnapshotsInProgress.get(state);
        final long clusterStateGeneration = getRepositoryGeneration(state, projectRepo);
        final boolean filesAreCurrent = clusterStateGeneration != RepositoryData.UNKNOWN_REPO_GEN
            && tracked.cache().getRepositoryGeneration() >= clusterStateGeneration;
        final Map<ShardId, List<StartedShardSnapshot>> startedSnapshotsOfShards = getStartedShardSnapshots(projectRepo);
        for (Snapshot snapshot : List.copyOf(runningShardSnapshots.keySet())) {
            if (snapshot.getProjectId().equals(projectRepo.projectId()) && snapshot.getRepository().equals(projectRepo.name())) {
                final boolean isInProgress = snapshotsInProgress.snapshot(snapshot) != null;
                // atomically with the shard snapshots that are added to it
                runningShardSnapshots.computeIfPresent(snapshot, (key, shards) -> {
                    shards.entrySet().removeIf(shard -> {
                        final var status = shard.getValue();
                        final boolean superseded = isSupersededByNewerSnapshot(
                            status,
                            startedSnapshotsOfShards.getOrDefault(shard.getKey(), List.of())
                        );
                        return isNoLongerNeeded(
                            shard.getKey(),
                            status,
                            tracked.cache(),
                            localShards,
                            superseded,
                            isInProgress == false && filesAreCurrent
                        );
                    });
                    return shards.isEmpty() ? null : shards;
                });
            }
        }
    }

    /**
     * A shard snapshot that has started uploading.
     *
     * @param creationTimeMillis when its status was created
     * @param startedFrom        the generation of the shard that it started from, or {@code null} if that is not known
     */
    private record StartedShardSnapshot(long creationTimeMillis, @Nullable ShardGeneration startedFrom) {}

    private Map<ShardId, List<StartedShardSnapshot>> getStartedShardSnapshots(ProjectRepo projectRepo) {
        final Map<ShardId, List<StartedShardSnapshot>> started = new HashMap<>();
        runningShardSnapshots.forEach((snapshot, shards) -> {
            if (snapshot.getProjectId().equals(projectRepo.projectId()) && snapshot.getRepository().equals(projectRepo.name())) {
                shards.forEach((shardId, status) -> {
                    switch (status.getStage()) {
                        case STARTED, FINALIZE, DONE -> started.computeIfAbsent(shardId, id -> new ArrayList<>())
                            .add(new StartedShardSnapshot(status.getCreationTimeMillis(), startedFromGenerations.get(status)));
                        // nothing has been uploaded yet, or what was is not used
                        case INIT, FAILURE, ABORTED, PAUSING, PAUSED -> {
                        }
                    }
                });
            }
        });
        return started;
    }

    /**
     * Whether a shard snapshot of the same shard started after the given one, from a generation that is not the one the given one
     * made. A shard snapshot that is queued behind another one of the shard starts from what that one made, even though it has not
     * been added to the repository yet, and then does not upload the files again, so the other one is still needed. One that starts
     * from anything else uploads them again, and counting both would understate the backlog. If it is not known where it started from,
     * it is not taken to be superseded, as that only overstates the backlog.
     */
    private static boolean isSupersededByNewerSnapshot(IndexShardSnapshotStatus status, List<StartedShardSnapshot> startedOfShard) {
        for (StartedShardSnapshot other : startedOfShard) {
            if (other.creationTimeMillis() > status.getCreationTimeMillis()
                && other.startedFrom() != null
                && other.startedFrom().equals(status.generation()) == false) {
                return true;
            }
        }
        return false;
    }

    private void retainStatusesOfRunningShardSnapshots() {
        final Set<IndexShardSnapshotStatus> statuses = getRunningShardSnapshotStatuses();
        doneSnapshotsSeenAt.keySet().retainAll(statuses);
        startedFromGenerations.keySet().retainAll(statuses);
    }

    /**
     * @return the generation of the repository in the cluster state, or {@link RepositoryData#UNKNOWN_REPO_GEN} if it is not there
     */
    private static long getRepositoryGeneration(ClusterState state, ProjectRepo projectRepo) {
        final ProjectMetadata project = state.metadata().projects().get(projectRepo.projectId());
        if (project == null) {
            return RepositoryData.UNKNOWN_REPO_GEN;
        }
        final RepositoryMetadata repository = RepositoriesMetadata.get(project).repository(projectRepo.name());
        return repository == null ? RepositoryData.UNKNOWN_REPO_GEN : repository.generation();
    }

    private Set<IndexShardSnapshotStatus> getRunningShardSnapshotStatuses() {
        final Set<IndexShardSnapshotStatus> statuses = new HashSet<>();
        runningShardSnapshots.values().forEach(shards -> statuses.addAll(shards.values()));
        return statuses;
    }

    /**
     * @param superseded               whether a newer snapshot of the shard has started from another generation than the one this made
     * @param cannotFinalizeAnyMore    whether the snapshot of the status is gone from the cluster state while the repository
     *                                 generations of the cache are as new as the one of the cluster state
     */
    private boolean isNoLongerNeeded(
        ShardId shardId,
        IndexShardSnapshotStatus status,
        RepositoryFilesCache cache,
        Set<ShardId> localShards,
        boolean superseded,
        boolean cannotFinalizeAnyMore
    ) {
        if (localShards.contains(shardId) == false) {
            doneSnapshotsSeenAt.remove(status);
            return true;
        }
        switch (status.getStage()) {
            case FAILURE, ABORTED, PAUSED -> {
                // nothing is reused from it, and a paused one is not going to go on on this node
                doneSnapshotsSeenAt.remove(status);
                return true;
            }
            case DONE -> {
                if (superseded) {
                    doneSnapshotsSeenAt.remove(status);
                    return true;
                }
                final RepositoryShardFiles files = cache.getShardFiles(shardId);
                if (files == null) {
                    return false;
                }
                final ShardGeneration seenAt = doneSnapshotsSeenAt.putIfAbsent(status, files.generation());
                final boolean caughtUp = Objects.equals(files.generation(), status.generation())
                    || (seenAt != null && Objects.equals(files.generation(), seenAt) == false);
                // If it is not in the files and the files are up to date, it did not finalize and cannot any more
                if (caughtUp || (cannotFinalizeAnyMore && cache.isUpToDate(shardId))) {
                    doneSnapshotsSeenAt.remove(status);
                    return true;
                }
                return false;
            }
            case INIT, STARTED, FINALIZE, PAUSING -> {
                // PAUSING is left to become PAUSED, which it does as soon as the shard snapshot has stopped
                return false;
            }
        }
        throw new AssertionError("unexpected stage " + status.getStage());
    }

    private void updateTrackedRepositories() {
        final Set<ProjectRepo> current = new HashSet<>();
        for (Repository repository : repositoriesService.getRepositories()) {
            if (repository instanceof BlobStoreRepository blobStoreRepository && blobStoreRepository.isReadOnly() == false) {
                final var projectRepo = blobStoreRepository.getProjectRepo();
                current.add(projectRepo);
                // a repository whose settings changed is a new instance, and may well point somewhere else
                final var tracked = trackedRepositories.get(projectRepo);
                if (tracked == null || tracked.repository() != blobStoreRepository) {
                    if (tracked != null) {
                        tracked.close();
                    }
                    trackedRepositories.put(projectRepo, newTrackedRepository(projectRepo, blobStoreRepository));
                }
            }
        }
        trackedRepositories.entrySet().removeIf(entry -> {
            if (current.contains(entry.getKey())) {
                return false;
            }
            entry.getValue().close();
            return true;
        });
        // the shard snapshots of a repository that is not tracked any more, e.g. it was deleted, are not needed by anybody
        runningShardSnapshots.keySet()
            .removeIf(
                snapshot -> trackedRepositories.containsKey(new ProjectRepo(snapshot.getProjectId(), snapshot.getRepository())) == false
            );
        retainStatusesOfRunningShardSnapshots();
    }

    private TrackedRepository newTrackedRepository(ProjectRepo projectRepo, BlobStoreRepository repository) {
        final var cache = new RepositoryFilesCache(
            projectRepo.name(),
            repository::getBlobStoreIndexShardSnapshots,
            repositoryReadExecutor,
            stateExecutor
        );
        final var refresher = new ShardGenerationsRefresher(
            projectRepo.name(),
            (shardIds, listener) -> getShardGenerationsFromMaster(projectRepo, shardIds, listener),
            cache,
            stateExecutor
        );
        return new TrackedRepository(repository, cache, refresher);
    }

    private void getShardGenerationsFromMaster(
        ProjectRepo projectRepo,
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
                new GetShardGenerationsRequest(evaluationInterval, projectRepo, shardIds),
                listener
            );
        }
    }

    /**
     * Picks up a new repository generation (a snapshot finished, or was deleted) and the shards that started on this node without
     * waiting for the next evaluation, but only if the repositories or the routing of this node changed. The work is not done on the
     * thread that applies the cluster state.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (enabled && (hasRepositoryMetadataChanged(event) || hasLocalPrimaryChanged(event))) {
            if (clusterStateRefreshQueued.compareAndSet(false, true)) {
                stateExecutor.execute(() -> {
                    clusterStateRefreshQueued.set(false);
                    refreshShardGenerations();
                });
            }
        }
    }

    private static boolean hasRepositoryMetadataChanged(ClusterChangedEvent event) {
        for (ProjectId projectId : event.state().metadata().projects().keySet()) {
            if (event.customMetadataChanged(projectId, RepositoriesMetadata.TYPE)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return whether a primary shard started on this node, or arrived with a new routing
     */
    private static boolean hasLocalPrimaryChanged(ClusterChangedEvent event) {
        if (event.routingTableChanged() == false) {
            return false;
        }
        final String localNodeId = event.state().nodes().getLocalNodeId();
        final RoutingNode localNode = event.state().getRoutingNodes().node(localNodeId);
        if (localNode == null) {
            return false;
        }
        final RoutingNode previousLocalNode = event.previousState().getRoutingNodes().node(localNodeId);
        for (ShardRouting shardRouting : localNode) {
            if (shardRouting.primary() && shardRouting.state() == ShardRoutingState.STARTED) {
                if (previousLocalNode == null || shardRouting.equals(previousLocalNode.getByShardId(shardRouting.shardId())) == false) {
                    return true;
                }
            }
        }
        return false;
    }

    private void refreshShardGenerations() {
        if (enabled == false) {
            return;
        }
        try {
            updateTrackedRepositories();
            if (trackedRepositories.isEmpty()) {
                return;
            }
            final Map<ProjectId, Set<ShardId>> shardIds = localShards.getShardIds();
            trackedRepositories.forEach(
                (projectRepo, tracked) -> tracked.refresher()
                    .refresh(
                        Trigger.CLUSTER_STATE,
                        tracked.repository().getMetadata().generation(),
                        shardIds.getOrDefault(projectRepo.projectId(), Set.of())
                    )
            );
        } catch (Exception e) {
            logger.warn("failed to refresh the shard generations for the snapshot backlog", e);
        }
    }

    /**
     * The started primary shards of the {@link IndicesService}
     */
    private static class IndicesLocalShards implements LocalShards {

        private final ClusterService clusterService;
        private final IndicesService indicesService;

        IndicesLocalShards(ClusterService clusterService, IndicesService indicesService) {
            this.clusterService = clusterService;
            this.indicesService = indicesService;
        }

        @Override
        public List<LocalShard> getShards() {
            final List<LocalShard> shards = new ArrayList<>();
            forEachStartedPrimary((shard, projectId) -> shards.add(new LocalShard(shard.shardId(), projectId, getCommitFiles(shard))));
            return shards;
        }

        @Override
        public Map<ProjectId, Set<ShardId>> getShardIds() {
            final Map<ProjectId, Set<ShardId>> shardIds = new HashMap<>();
            forEachStartedPrimary((shard, projectId) -> shardIds.computeIfAbsent(projectId, id -> new HashSet<>()).add(shard.shardId()));
            return shardIds;
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
         * @return the names and lengths of the files of the latest commit of the shard, without flushing the shard as a snapshot would,
         *         or {@code null} if they are not available at the moment.
         */
        @Nullable
        private static Map<String, Long> getCommitFiles(IndexShard shard) {
            try (var commitRef = shard.acquireLastIndexCommit(false)) {
                return SnapshotBacklogTracker.getCommitFiles(commitRef.getIndexCommit());
            } catch (Exception e) {
                // e.g. the shard is closing
                logger.debug(() -> "cannot get the commit files of " + shard.shardId(), e);
                return null;
            }
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
}
