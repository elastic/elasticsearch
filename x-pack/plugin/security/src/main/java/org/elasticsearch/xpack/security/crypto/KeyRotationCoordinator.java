/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

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
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandler;
import org.elasticsearch.xpack.security.spi.encryption.EncryptionService;

import java.io.Closeable;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Owns the primary encryption key (PEK) lifecycle on the elected master: installs the initial key, rotates the active key on a timer,
 * drives registered {@link EncryptedDataHandler}s to re-encrypt their owned data, and retires non-active keys once their grace window
 * expires.
 *
 * <p>Re-encryption follows a two-phase compute-then-CAS pattern:
 * <ol>
 *     <li>For each handler whose {@code handlerKeyIds} entry is not yet on the current {@code activeKeyId}, the coordinator forks to
 *     the generic thread pool, snapshots cluster state, and asks the handler to produce a re-encrypted copy of its
 *     {@link Metadata.ProjectCustom} slice.</li>
 *     <li>The result is submitted as a {@link ReEncryptApplyTask} which, on the master thread, atomically swaps the handler's custom
 *     and updates {@code handlerKeyIds} — but only if the snapshot the compute phase ran against is still current
 *     (slice unchanged and {@code activeKeyId} unchanged). On conflict the task is a no-op and the next tick re-attempts.</li>
 * </ol>
 */
public class KeyRotationCoordinator implements LocalNodeMasterListener, Closeable {

    private static final Logger logger = LogManager.getLogger(KeyRotationCoordinator.class);

    // Number of check_intervals (= scrub passes) a non-active key persists after its deactivation time before retirement.
    // 10 leaves headroom for cluster-state propagation, in-flight writes, and one full handler walk — all typically << one tick.
    private static final int GRACE_TICKS = 10;

    /**
     * How frequently the primary encryption key is rotated. {@link TimeValue#ZERO} disables rotation.
     */
    public static final Setting<TimeValue> ROTATION_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.security.encryption.key_rotation.interval",
        TimeValue.timeValueDays(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * How often the master polls to drive rotation forward (begin a new rotation if due, resume
     * an in-progress rotation, or retire old keys when handlers have all completed).
     */
    public static final Setting<TimeValue> CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.security.encryption.key_rotation.check_interval",
        TimeValue.timeValueHours(1),
        new Setting.Validator<>() {
            @Override
            public void validate(TimeValue value) {
                if (value.compareTo(TimeValue.timeValueSeconds(1)) < 0) {
                    throw new IllegalArgumentException(
                        "[xpack.security.encryption.key_rotation.check_interval] must be at least 1s, got [" + value + "]"
                    );
                }
            }

            @Override
            public void validate(TimeValue value, Map<Setting<?>, Object> settings) {
                TimeValue rotationInterval = (TimeValue) settings.get(ROTATION_INTERVAL_SETTING);
                if (rotationInterval.duration() > 0 && value.compareTo(rotationInterval) > 0) {
                    throw new IllegalArgumentException(
                        "[xpack.security.encryption.key_rotation.check_interval] ("
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
    private final AtomicBoolean rotating = new AtomicBoolean(false);
    // Used for logging when a rotation is in progress for an unexpectedly long time. Same access pattern as `rotating`.
    private final AtomicLong rotatingSince = new AtomicLong(0L);

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
        this.taskQueue = clusterService.createTaskQueue(
            "primary-encryption-key",
            Priority.NORMAL,
            new KeyRotationExecutor(projectResolver, threadPool)
        );
        this.rotationInterval = rotationInterval;
        this.checkInterval = checkInterval;
    }

    @Override
    public void onMaster() {
        startSchedule();
    }

    @Override
    public void offMaster() {
        rotating.set(false);
        rotatingSince.set(0L);
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
        // Install runs regardless of whether rotation is enabled — we always want a key once the cluster is ready.
        if (getCurrentMetadata(state) == null && checkPekFeatureAvailable(state)) {
            submitInstallKey();
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

        PrimaryEncryptionKeyMetadata metadata = getCurrentMetadata(state);
        if (metadata == null) {
            return;
        }

        long now = threadPool.absoluteTimeInMillis();
        long activeKeyAge = now - metadata.getGeneratedAt(metadata.getActiveKeyId());

        rotate(metadata, now);

        if (activeKeyAge >= rotationInterval.millis()) {
            logger.info("primary encryption key due for rotation (active key generated {} ago)", TimeValue.timeValueMillis(activeKeyAge));
            submitBeginRotation();
        }

        long retireCutoff = now - GRACE_TICKS * checkInterval.millis();
        if (metadata.findRetireableKeyIds(retireCutoff).isEmpty() == false) {
            submitRetireKeys(retireCutoff);
        }
    }

    private void rotate(PrimaryEncryptionKeyMetadata metadata, long now) {
        String activeKeyId = metadata.getActiveKeyId();
        List<EncryptedDataHandler<?>> pending = handlers.stream().filter(h -> metadata.isHandlerOnActive(h.customName()) == false).toList();
        if (pending.isEmpty()) {
            return;
        }
        if (rotating.compareAndSet(false, true) == false) {
            logger.warn(
                "rotation already in progress, skipping this tick (in progress for {})",
                TimeValue.timeValueMillis(now - rotatingSince.get())
            );
            return;
        }
        rotatingSince.set(now);
        try (
            var listeners = new RefCountingListener(
                ActionListener.runAfter(
                    ActionListener.wrap(unused -> {}, e -> logger.warn("re-encryption failed; will retry on next tick", e)),
                    () -> {
                        rotatingSince.set(0L);
                        rotating.set(false);
                    }
                )
            )
        ) {
            for (EncryptedDataHandler<?> handler : pending) {
                ActionListener<Void> l = listeners.acquire();
                threadPool.generic().execute(() -> dispatchOne(handler, activeKeyId, l));
            }
        }
    }

    private <T extends Metadata.ProjectCustom> void dispatchOne(
        EncryptedDataHandler<T> handler,
        String expectedActiveKeyId,
        ActionListener<Void> listener
    ) {
        try {
            ClusterState snapshot = clusterService.state();
            ProjectState projectState = projectResolver.getProjectState(snapshot);

            T oldCustom = projectState.metadata().custom(handler.customName());
            T newCustom = handler.reEncrypt(oldCustom, encryptionService, expectedActiveKeyId);
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
                "re-encrypt-" + handler.customName(),
                new ReEncryptApplyTask(handler.customName(), oldCustom, newCustom, expectedActiveKeyId, listener),
                null
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
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
    public PrimaryEncryptionKeyMetadata getCurrentMetadata(ClusterState state) {
        return projectResolver.getProjectState(state).metadata().custom(PrimaryEncryptionKeyMetadata.TYPE);
    }

    private boolean checkPekFeatureAvailable(ClusterState state) {
        if (featureService.clusterHasFeature(state, PrimaryEncryptionKeyService.PRIMARY_ENCRYPTION_KEY_FEATURE) == false) {
            logger.debug("not all nodes support primary encryption key feature, waiting for rolling upgrade to complete");
            return false;
        }
        return true;
    }

    void submitInstallKey() {
        taskQueue.submitTask("install-primary-encryption-key", new InstallKeyTask(), null);
    }

    void submitBeginRotation() {
        taskQueue.submitTask("begin-primary-encryption-key-rotation", new BeginRotationTask(), null);
    }

    void submitRetireKeys(long cutoffDeactivationMillis) {
        taskQueue.submitTask("retire-primary-encryption-keys", new RetireKeysTask(cutoffDeactivationMillis), null);
    }

    /**
     * Hierarchy of cluster-state tasks that flow through the PEK task queue. {@link KeyRotationExecutor} dispatches on the concrete type.
     */
    sealed interface KeyRotationTask extends ClusterStateTaskListener permits InstallKeyTask, BeginRotationTask, RetireKeysTask,
        ReEncryptApplyTask {

        String description();

        @Override
        default void onFailure(@Nullable Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.ERROR, () -> "failure during " + description(), e);
        }
    }

    static final class InstallKeyTask implements KeyRotationTask {
        @Override
        public String description() {
            return "primary encryption key initial install";
        }
    }

    static final class BeginRotationTask implements KeyRotationTask {
        @Override
        public String description() {
            return "primary encryption key rotation begin";
        }
    }

    record RetireKeysTask(long cutoffDeactivationMillis) implements KeyRotationTask {
        @Override
        public String description() {
            return "primary encryption key retire (cutoff=" + cutoffDeactivationMillis + ")";
        }
    }

    /**
     * Atomically swap a handler's {@link Metadata.ProjectCustom} for a re-encrypted copy and record progress in
     * {@code handlerKeyIds}. Conflict cases (slice changed, or a new rotation began since compute) are turned into no-ops; the next
     * tick will re-dispatch.
     */
    record ReEncryptApplyTask(
        String customName,
        Metadata.ProjectCustom expectedOld,
        Metadata.ProjectCustom newCustom,
        String expectedActiveKeyId,
        ActionListener<Void> completionListener
    ) implements KeyRotationTask {
        @Override
        public String description() {
            return "primary encryption key re-encrypt [" + customName + "]";
        }

        @Override
        public void onFailure(Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.ERROR, () -> "failure during " + description(), e);
            completionListener.onFailure(e);
        }
    }

    static class KeyRotationExecutor extends SimpleBatchedExecutor<KeyRotationTask, Void> {
        private static final SecureRandom RANDOM = new SecureRandom();

        private final ProjectResolver projectResolver;
        private final ThreadPool threadPool;

        KeyRotationExecutor(ProjectResolver projectResolver, ThreadPool threadPool) {
            this.projectResolver = projectResolver;
            this.threadPool = threadPool;
        }

        @Override
        public Tuple<ClusterState, Void> executeTask(KeyRotationTask task, ClusterState clusterState) {
            ProjectState projectState = projectResolver.getProjectState(clusterState);
            PrimaryEncryptionKeyMetadata existing = projectState.metadata().custom(PrimaryEncryptionKeyMetadata.TYPE);

            return switch (task) {
                case InstallKeyTask ignored -> executeInstallInitial(clusterState, projectState, existing);
                case BeginRotationTask ignored -> executeBeginRotation(clusterState, projectState, existing);
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
            PrimaryEncryptionKeyMetadata existing
        ) {
            if (existing != null) {
                // Initial install is idempotent: if metadata already exists, leave it alone.
                return Tuple.tuple(clusterState, null);
            }

            byte[] keyBytes = randomKey();
            String keyId = PrimaryEncryptionKeyMetadata.generateKeyId();
            PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
                Map.of(keyId, new PrimaryEncryptionKeyMetadata.KeyEntry(keyBytes, threadPool.absoluteTimeInMillis())),
                keyId
            );
            logger.info("installing primary encryption key [{}]", keyId);
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeBeginRotation(
            ClusterState clusterState,
            ProjectState projectState,
            PrimaryEncryptionKeyMetadata existing
        ) {
            if (existing == null) {
                logger.warn("ignoring begin-rotation task because no primary encryption key is installed");
                return Tuple.tuple(clusterState, null);
            }
            byte[] keyBytes = randomKey();
            String newKeyId = PrimaryEncryptionKeyMetadata.generateKeyId();
            Map<String, PrimaryEncryptionKeyMetadata.KeyEntry> newEntries = new HashMap<>(existing.getKeys());
            newEntries.put(newKeyId, new PrimaryEncryptionKeyMetadata.KeyEntry(keyBytes, threadPool.absoluteTimeInMillis()));
            PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(newEntries, newKeyId, existing.getHandlerKeyIds());
            logger.info("beginning primary encryption key rotation: new active key [{}]", newKeyId);
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeRetireKeys(
            ClusterState clusterState,
            ProjectState projectState,
            PrimaryEncryptionKeyMetadata existing,
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
            Map<String, PrimaryEncryptionKeyMetadata.KeyEntry> retained = new HashMap<>(existing.getKeys());
            retained.keySet().removeAll(retiredIds);
            PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(retained, activeKeyId, existing.getHandlerKeyIds());
            logger.info("primary encryption key retire: retained active key [{}], retired keys {}", activeKeyId, new TreeSet<>(retiredIds));
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeReEncryptApply(
            ClusterState clusterState,
            ProjectState projectState,
            PrimaryEncryptionKeyMetadata existing,
            ReEncryptApplyTask task
        ) {
            if (existing == null) {
                // PEK metadata vanished. Drop and let next tick re-attempt.
                logger.debug("dropping re-encrypt task for [{}]: no PEK metadata installed", task.customName());
                return Tuple.tuple(clusterState, null);
            }
            if (existing.getActiveKeyId().equals(task.expectedActiveKeyId()) == false) {
                // A new rotation began since the compute phase. The re-encrypted custom is stale; drop and re-tick.
                logger.debug(
                    "dropping re-encrypt task for [{}]: activeKeyId drifted from [{}] to [{}]",
                    task.customName(),
                    task.expectedActiveKeyId(),
                    existing.getActiveKeyId()
                );
                return Tuple.tuple(clusterState, null);
            }
            Metadata.ProjectCustom current = projectState.metadata().custom(task.customName());
            if (current != task.expectedOld()) {
                // Slice was modified between snapshot and apply. Drop and re-tick.
                logger.debug("dropping re-encrypt task for [{}]: slice mutated between compute and apply", task.customName());
                return Tuple.tuple(clusterState, null);
            }
            PrimaryEncryptionKeyMetadata updatedPek = existing.withHandlerKeyId(task.customName(), task.expectedActiveKeyId());
            ClusterState newState = clusterState.copyAndUpdateProject(
                projectState.projectId(),
                b -> b.putCustom(task.customName(), task.newCustom()).putCustom(PrimaryEncryptionKeyMetadata.TYPE, updatedPek)
            );
            return Tuple.tuple(newState, null);
        }

        private static ClusterState putMetadata(
            ClusterState clusterState,
            ProjectState projectState,
            PrimaryEncryptionKeyMetadata metadata
        ) {
            return clusterState.copyAndUpdateProject(
                projectState.projectId(),
                b -> b.putCustom(PrimaryEncryptionKeyMetadata.TYPE, metadata)
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
            return "primary encryption key tasks " + byType + " [" + tasks.size() + " pending]";
        }
    }
}
