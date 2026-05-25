/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.Closeable;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Owns the project encryption key (PEK) lifecycle on the elected master, fanned out across every project in cluster state. Per project,
 * the coordinator installs the initial key, rotates the active key on a timer, drives registered {@link EncryptedDataHandler}s to
 * re-encrypt their owned data, and retires non-active keys once their grace window expires.
 *
 * <p>Every task that flows through the coordinator's task queue carries a {@link ProjectId} so the executor mutates exactly the named
 * project's slice — never relying on a {@link ProjectResolver} fallback to pick which project to operate on.
 *
 * <p>Re-encryption follows a two-phase compute-then-CAS pattern (per project, per handler):
 * <ol>
 *     <li>For each project whose {@code handlerKeyIds} entry is not yet on the project's {@code activeKeyId}, the coordinator forks to
 *     the generic thread pool, snapshots cluster state, and asks the handler to produce a re-encrypted copy of its
 *     {@link Metadata.ProjectCustom} slice. The handler is invoked inside {@link ProjectResolver#executeOnProject} so any calls it
 *     makes back into {@link EncryptionService} resolve to the correct project.</li>
 *     <li>The result is submitted as a {@link ReEncryptApplyTask} which, on the master thread, atomically swaps the project's
 *     handler custom and updates {@code handlerKeyIds} — but only if the snapshot the compute phase ran against is still current
 *     (slice unchanged and {@code activeKeyId} unchanged). On conflict the task is a no-op and the next tick re-attempts.</li>
 * </ol>
 */
public class KeyRotationCoordinator implements LocalNodeMasterListener, Closeable {

    private static final Logger logger = LogManager.getLogger(KeyRotationCoordinator.class);

    // Number of check_intervals (= scrub passes) a non-active key persists after its deactivation time before retirement.
    // 10 leaves headroom for cluster-state propagation, in-flight writes, and one full handler walk — all typically << one tick.
    private static final int GRACE_TICKS = 10;

    /**
     * How frequently the project encryption key is rotated. {@link TimeValue#ZERO} disables rotation.
     */
    public static final Setting<TimeValue> ROTATION_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.encryption.key_rotation.interval",
        TimeValue.timeValueDays(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * How often the master polls to drive rotation forward (begin a new rotation if due, resume
     * an in-progress rotation, or retire old keys when handlers have all completed).
     */
    public static final Setting<TimeValue> CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.encryption.key_rotation.check_interval",
        TimeValue.timeValueHours(1),
        new Setting.Validator<>() {
            @Override
            public void validate(TimeValue value) {
                if (value.compareTo(TimeValue.timeValueSeconds(1)) < 0) {
                    throw new IllegalArgumentException(
                        "[xpack.encryption.key_rotation.check_interval] must be at least 1s, got [" + value + "]"
                    );
                }
            }

            @Override
            public void validate(TimeValue value, Map<Setting<?>, Object> settings) {
                TimeValue rotationInterval = (TimeValue) settings.get(ROTATION_INTERVAL_SETTING);
                if (rotationInterval.duration() > 0 && value.compareTo(rotationInterval) > 0) {
                    throw new IllegalArgumentException(
                        "[xpack.encryption.key_rotation.check_interval] ("
                            + value
                            + ") must not be greater than ["
                            + ROTATION_INTERVAL_SETTING.getKey()
                            + "] ("
                            + rotationInterval
                            + ")"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(ROTATION_INTERVAL_SETTING).iterator();
            }
        },
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;
    private final FeatureService featureService;
    private final EncryptionService encryptionService;
    private final MasterServiceTaskQueue<KeyRotationTask> taskQueue;
    private final TimeValue rotationInterval;
    private final TimeValue checkInterval;
    private final List<EncryptedDataHandler<?>> handlers;

    private Scheduler.Cancellable scheduledTask;
    private volatile boolean closed = false;
    // Tracks which projects currently have an in-flight re-encrypt fan-out and when that fan-out started (millis since epoch). Per-project
    // gating means a slow project does not block other projects' ticks. The value is purely diagnostic (used in the "rotation in progress
    // for X ms" warn log).
    private final ConcurrentHashMap<ProjectId, Long> rotatingSinceByProject = new ConcurrentHashMap<>();

    public static KeyRotationCoordinator create(
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        FeatureService featureService,
        EncryptionService encryptionService,
        Collection<EncryptedDataHandler<?>> handlers,
        Settings settings
    ) {
        TimeValue rotationInterval = ROTATION_INTERVAL_SETTING.get(settings);
        TimeValue checkInterval = CHECK_INTERVAL_SETTING.get(settings);
        KeyRotationCoordinator coordinator = new KeyRotationCoordinator(
            clusterService,
            threadPool,
            projectResolver,
            featureService,
            encryptionService,
            handlers,
            rotationInterval,
            checkInterval
        );
        clusterService.addLocalNodeMasterListener(coordinator);
        clusterService.addListener(coordinator::onClusterStateChanged);
        return coordinator;
    }

    KeyRotationCoordinator(
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        FeatureService featureService,
        EncryptionService encryptionService,
        Collection<EncryptedDataHandler<?>> handlers,
        TimeValue rotationInterval,
        TimeValue checkInterval
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.featureService = featureService;
        this.encryptionService = encryptionService;
        this.handlers = new CopyOnWriteArrayList<>(handlers);
        this.taskQueue = clusterService.createTaskQueue("project-encryption-key", Priority.NORMAL, new KeyRotationExecutor(threadPool));
        this.rotationInterval = rotationInterval;
        this.checkInterval = checkInterval;
    }

    @Override
    public void onMaster() {
        startSchedule();
    }

    @Override
    public void offMaster() {
        rotatingSinceByProject.clear();
        stopSchedule();
    }

    // package-private for testing
    void onClusterStateChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }
        if (checkPekFeatureAvailable(state) == false) {
            return;
        }
        // Install runs regardless of whether rotation is enabled — we always want a key once the cluster is ready, for every project.
        for (ProjectMetadata project : state.metadata().projects().values()) {
            if (project.custom(ProjectEncryptionKeyMetadata.TYPE) == null) {
                submitInstallKey(project.id());
            }
        }
    }

    private boolean rotationDisabled() {
        return rotationInterval.duration() == 0;
    }

    private synchronized void startSchedule() {
        if (closed || rotationDisabled() || scheduledTask != null) {
            return;
        }
        logger.debug("starting key rotation schedule (rotation_interval={}, check_interval={})", rotationInterval, checkInterval);
        scheduledTask = threadPool.scheduleWithFixedDelay(this::tick, checkInterval, threadPool.generic());
    }

    private synchronized void stopSchedule() {
        if (scheduledTask != null) {
            logger.debug("stopping key rotation schedule");
            scheduledTask.cancel();
            scheduledTask = null;
        }
    }

    void tick() {
        if (closed || rotationDisabled()) {
            return;
        }
        ClusterState state = clusterService.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        long now = threadPool.absoluteTimeInMillis();
        for (ProjectMetadata project : state.metadata().projects().values()) {
            tickProject(project, now);
        }
    }

    private void tickProject(ProjectMetadata project, long now) {
        ProjectId projectId = project.id();
        ProjectEncryptionKeyMetadata metadata = project.custom(ProjectEncryptionKeyMetadata.TYPE);
        if (metadata == null) {
            return;
        }

        long activeKeyAge = now - metadata.getGeneratedAt(metadata.getActiveKeyId());

        rotate(projectId, metadata, now);

        if (activeKeyAge >= rotationInterval.millis()) {
            logger.info(
                "project [{}] encryption key due for rotation (active key generated {} ago)",
                projectId,
                TimeValue.timeValueMillis(activeKeyAge)
            );
            submitBeginRotation(projectId);
        }

        long retireCutoff = now - GRACE_TICKS * checkInterval.millis();
        if (metadata.findRetireableKeyIds(retireCutoff).isEmpty() == false) {
            submitRetireKeys(projectId, retireCutoff);
        }
    }

    private void rotate(ProjectId projectId, ProjectEncryptionKeyMetadata metadata, long now) {
        String activeKeyId = metadata.getActiveKeyId();
        List<EncryptedDataHandler<?>> pending = handlers.stream().filter(h -> metadata.isHandlerOnActive(h.customName()) == false).toList();
        if (pending.isEmpty()) {
            return;
        }
        Long existingStart = rotatingSinceByProject.putIfAbsent(projectId, now);
        if (existingStart != null) {
            logger.warn(
                "rotation already in progress for project [{}], skipping this tick (in progress for {})",
                projectId,
                TimeValue.timeValueMillis(now - existingStart)
            );
            return;
        }
        try (
            var listeners = new RefCountingListener(
                ActionListener.runAfter(
                    ActionListener.wrap(
                        unused -> {},
                        e -> logger.warn(() -> "re-encryption failed for project [" + projectId + "]; will retry on next tick", e)
                    ),
                    () -> rotatingSinceByProject.remove(projectId)
                )
            )
        ) {
            for (EncryptedDataHandler<?> handler : pending) {
                ActionListener<Void> l = listeners.acquire();
                threadPool.generic().execute(() -> dispatchOne(projectId, handler, activeKeyId, l));
            }
        }
    }

    private <T extends Metadata.ProjectCustom> void dispatchOne(
        ProjectId projectId,
        EncryptedDataHandler<T> handler,
        String expectedActiveKeyId,
        ActionListener<Void> listener
    ) {
        try {
            ClusterState snapshot = clusterService.state();
            ProjectMetadata project = snapshot.metadata().projects().get(projectId);
            if (project == null) {
                // Project vanished between schedule and dispatch. Drop and let the next tick reconcile.
                listener.onResponse(null);
                return;
            }
            T oldCustom = project.custom(handler.customName());
            // Wrap the handler invocation so any encrypt/decrypt the handler issues resolves keys for `projectId`. The generic-thread we
            // forked to has no project header in its ThreadContext, so the resolver would otherwise fall back to the default project.
            T newCustom = invokeHandler(projectId, handler, oldCustom, expectedActiveKeyId);
            if (newCustom == null || newCustom == oldCustom) {
                // Nothing to do — handler signaled no change.
                listener.onResponse(null);
                return;
            }
            assert handler.customName().equals(newCustom.getWriteableName())
                : "handler ["
                    + handler.getClass().getSimpleName()
                    + "] customName="
                    + handler.customName()
                    + " does not match returned custom getWriteableName="
                    + newCustom.getWriteableName();
            taskQueue.submitTask(
                "re-encrypt-" + handler.customName() + "[" + projectId + "]",
                new ReEncryptApplyTask(projectId, handler.customName(), oldCustom, newCustom, expectedActiveKeyId, listener),
                null
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private <T extends Metadata.ProjectCustom> T invokeHandler(
        ProjectId projectId,
        EncryptedDataHandler<T> handler,
        T oldCustom,
        String expectedActiveKeyId
    ) {
        // Single-slot holder lets us return the handler's result out of the executeOnProject lambda without losing the generic type.
        AtomicReference<T> result = new AtomicReference<>();
        projectResolver.executeOnProject(projectId, () -> result.set(handler.reEncrypt(oldCustom, encryptionService, expectedActiveKeyId)));
        return result.get();
    }

    @Override
    public synchronized void close() {
        closed = true;
        clusterService.removeListener(this);
        stopSchedule();
    }

    // visible for testing
    void register(EncryptedDataHandler<?> handler) {
        handlers.add(handler);
        logger.debug("registered encrypted-data handler [{}]", handler.getClass().getSimpleName());
    }

    @Nullable
    public ProjectEncryptionKeyMetadata getCurrentMetadata(ClusterState state, ProjectId projectId) {
        ProjectMetadata project = state.metadata().projects().get(projectId);
        return project != null ? project.custom(ProjectEncryptionKeyMetadata.TYPE) : null;
    }

    private boolean checkPekFeatureAvailable(ClusterState state) {
        if (featureService.clusterHasFeature(state, ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE) == false) {
            logger.debug("not all nodes support project encryption key feature, waiting for rolling upgrade to complete");
            return false;
        }
        return true;
    }

    void submitInstallKey(ProjectId projectId) {
        taskQueue.submitTask("install-project-encryption-key[" + projectId + "]", new InstallKeyTask(projectId), null);
    }

    void submitBeginRotation(ProjectId projectId) {
        taskQueue.submitTask("begin-project-encryption-key-rotation[" + projectId + "]", new BeginRotationTask(projectId), null);
    }

    void submitRetireKeys(ProjectId projectId, long cutoffDeactivationMillis) {
        taskQueue.submitTask(
            "retire-project-encryption-keys[" + projectId + "]",
            new RetireKeysTask(projectId, cutoffDeactivationMillis),
            null
        );
    }

    /**
     * Hierarchy of cluster-state tasks that flow through the PEK task queue. Each task carries the {@link ProjectId} it targets;
     * {@link KeyRotationExecutor} dispatches on the concrete type.
     */
    sealed interface KeyRotationTask extends ClusterStateTaskListener permits InstallKeyTask, BeginRotationTask, RetireKeysTask,
        ReEncryptApplyTask {

        ProjectId projectId();

        String description();

        @Override
        default void onFailure(@Nullable Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.ERROR, () -> "failure during " + description(), e);
        }
    }

    record InstallKeyTask(ProjectId projectId) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key initial install [" + projectId + "]";
        }
    }

    record BeginRotationTask(ProjectId projectId) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key rotation begin [" + projectId + "]";
        }
    }

    record RetireKeysTask(ProjectId projectId, long cutoffDeactivationMillis) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key retire [" + projectId + "] (cutoff=" + cutoffDeactivationMillis + ")";
        }
    }

    /**
     * Atomically swap a project's handler {@link Metadata.ProjectCustom} for a re-encrypted copy and record progress in
     * {@code handlerKeyIds}. Conflict cases (project gone, slice changed, or a new rotation began since compute) are turned into no-ops;
     * the next tick will re-dispatch.
     */
    record ReEncryptApplyTask(
        ProjectId projectId,
        String customName,
        Metadata.ProjectCustom expectedOld,
        Metadata.ProjectCustom newCustom,
        String expectedActiveKeyId,
        ActionListener<Void> completionListener
    ) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key re-encrypt [" + projectId + "][" + customName + "]";
        }

        @Override
        public void onFailure(Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.ERROR, () -> "failure during " + description(), e);
            completionListener.onFailure(e);
        }
    }

    static class KeyRotationExecutor extends SimpleBatchedExecutor<KeyRotationTask, Void> {
        private static final SecureRandom RANDOM = new SecureRandom();

        private final ThreadPool threadPool;

        KeyRotationExecutor(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public Tuple<ClusterState, Void> executeTask(KeyRotationTask task, ClusterState clusterState) {
            if (clusterState.metadata().hasProject(task.projectId()) == false) {
                // Project was deleted between submission and execution. Drop the task as a no-op.
                logger.debug("dropping [{}]: project no longer present in cluster state", task.description());
                return Tuple.tuple(clusterState, null);
            }
            ProjectState projectState = clusterState.projectState(task.projectId());
            ProjectEncryptionKeyMetadata existing = projectState.metadata().custom(ProjectEncryptionKeyMetadata.TYPE);

            return switch (task) {
                case InstallKeyTask install -> executeInstallInitial(clusterState, projectState, existing);
                case BeginRotationTask begin -> executeBeginRotation(clusterState, projectState, existing);
                case RetireKeysTask retireKeysTask -> executeRetireKeys(
                    clusterState,
                    projectState,
                    existing,
                    retireKeysTask.cutoffDeactivationMillis()
                );
                case ReEncryptApplyTask reEncryptTask -> executeReEncryptApply(clusterState, projectState, existing, reEncryptTask);
            };
        }

        private Tuple<ClusterState, Void> executeInstallInitial(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing
        ) {
            if (existing != null) {
                // Initial install is idempotent: if metadata already exists, leave it alone.
                return Tuple.tuple(clusterState, null);
            }

            byte[] keyBytes = randomKey();
            String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
                Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(keyBytes, threadPool.absoluteTimeInMillis())),
                keyId
            );
            logger.info("installing project encryption key [{}] for project [{}]", keyId, projectState.projectId());
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeBeginRotation(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing
        ) {
            if (existing == null) {
                logger.warn(
                    "ignoring begin-rotation task because no project encryption key is installed for project [{}]",
                    projectState.projectId()
                );
                return Tuple.tuple(clusterState, null);
            }
            byte[] keyBytes = randomKey();
            String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
            Map<String, ProjectEncryptionKeyMetadata.KeyEntry> newEntries = new HashMap<>(existing.getKeys());
            newEntries.put(newKeyId, new ProjectEncryptionKeyMetadata.KeyEntry(keyBytes, threadPool.absoluteTimeInMillis()));
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(newEntries, newKeyId, existing.getHandlerKeyIds());
            logger.info(
                "beginning project encryption key rotation for project [{}]: new active key [{}]",
                projectState.projectId(),
                newKeyId
            );
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeRetireKeys(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            long cutoffDeactivationMillis
        ) {
            if (existing == null) {
                return Tuple.tuple(clusterState, null);
            }
            Set<String> retiredIds = existing.findRetireableKeyIds(cutoffDeactivationMillis);
            if (retiredIds.isEmpty()) {
                return Tuple.tuple(clusterState, null);
            }
            String activeKeyId = existing.getActiveKeyId();
            Map<String, ProjectEncryptionKeyMetadata.KeyEntry> retained = new HashMap<>(existing.getKeys());
            retained.keySet().removeAll(retiredIds);
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(retained, activeKeyId, existing.getHandlerKeyIds());
            logger.info(
                "project encryption key retire for project [{}]: retained active key [{}], retired keys {}",
                projectState.projectId(),
                activeKeyId,
                new TreeSet<>(retiredIds)
            );
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeReEncryptApply(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            ReEncryptApplyTask task
        ) {
            if (existing == null) {
                // PEK metadata vanished. Drop and let next tick re-attempt.
                logger.debug("dropping re-encrypt task for [{}]: no PEK metadata installed", task.description());
                return Tuple.tuple(clusterState, null);
            }
            if (existing.getActiveKeyId().equals(task.expectedActiveKeyId()) == false) {
                // A new rotation began since the compute phase. The re-encrypted custom is stale; drop and re-tick.
                logger.debug(
                    "dropping re-encrypt task for [{}]: activeKeyId drifted from [{}] to [{}]",
                    task.description(),
                    task.expectedActiveKeyId(),
                    existing.getActiveKeyId()
                );
                return Tuple.tuple(clusterState, null);
            }
            Metadata.ProjectCustom current = projectState.metadata().custom(task.customName());
            if (current != task.expectedOld()) {
                // Slice was modified between snapshot and apply. Drop and re-tick.
                logger.debug("dropping re-encrypt task for [{}]: slice mutated between compute and apply", task.description());
                return Tuple.tuple(clusterState, null);
            }
            ProjectEncryptionKeyMetadata updatedPek = existing.withHandlerKeyId(task.customName(), task.expectedActiveKeyId());
            ClusterState newState = clusterState.copyAndUpdateProject(
                projectState.projectId(),
                b -> b.putCustom(task.customName(), task.newCustom()).putCustom(ProjectEncryptionKeyMetadata.TYPE, updatedPek)
            );
            return Tuple.tuple(newState, null);
        }

        private static ClusterState putMetadata(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata metadata
        ) {
            return clusterState.copyAndUpdateProject(
                projectState.projectId(),
                b -> b.putCustom(ProjectEncryptionKeyMetadata.TYPE, metadata)
            );
        }

        private static byte[] randomKey() {
            byte[] keyBytes = new byte[32];
            RANDOM.nextBytes(keyBytes);
            return keyBytes;
        }

        @Override
        public void taskSucceeded(KeyRotationTask task, Void unused) {
            logger.debug("[{}] succeeded", task.description());
            if (task instanceof ReEncryptApplyTask reEncrypt) {
                reEncrypt.completionListener().onResponse(null);
            }
        }

        @Override
        public String describeTasks(List<KeyRotationTask> tasks) {
            Map<String, Long> byType = tasks.stream().collect(Collectors.groupingBy(KeyRotationTask::description, Collectors.counting()));
            return "project encryption key tasks " + byType + " [" + tasks.size() + " pending]";
        }
    }
}
