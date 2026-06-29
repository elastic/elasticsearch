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
import org.elasticsearch.cluster.ClusterStateListener;
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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Owns the PEK lifecycle on the elected master: installs the initial key, rotates the active key on a timer, updates
 * {@link ProjectEncryptionKeyMetadata#getPasswordId()} when the active password changes, drives registered
 * {@link EncryptedDataHandler}s to re-encrypt their data, and retires expired non-active keys.
 *
 * <p>Re-encryption uses a compute-then-CAS pattern: each handler re-encrypts against a cluster-state snapshot on the generic pool, then
 * a {@link ReEncryptApplyTask} atomically applies the result — aborting if the snapshot is stale. Conflicts retry on the next tick.
 */
class KeyRotationCoordinator implements LocalNodeMasterListener, Closeable {

    private static final Logger logger = LogManager.getLogger(KeyRotationCoordinator.class);

    // Number of check_intervals (= scrub passes) a non-active key persists after its deactivation time before retirement.
    // 10 leaves headroom for cluster-state propagation, in-flight writes, and one full handler walk — all typically << one tick.
    private static final int GRACE_TICKS = 10;

    /**
     * How frequently the project encryption key is rotated. {@link TimeValue#ZERO} disables rotation.
     */
    static final Setting<TimeValue> ROTATION_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.encryption.key_rotation.interval",
        TimeValue.timeValueDays(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * How often the master polls to drive rotation forward (begin a new rotation if due, resume
     * an in-progress rotation, or retire old keys when handlers have all completed).
     */
    static final Setting<TimeValue> CHECK_INTERVAL_SETTING = Setting.timeSetting(
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
    private final ProjectEncryptionKeyMetadata.PekEncryption pekEncryption;
    private final ClusterStateListener clusterStateListener = this::onClusterStateChanged;
    private final Supplier<Settings> settingsSupplier;

    private volatile Scheduler.Cancellable scheduledTask;
    private volatile boolean closed = false;
    private final AtomicBoolean rotating = new AtomicBoolean(false);
    private final AtomicBoolean installInFlight = new AtomicBoolean(false);
    private final AtomicBoolean beginRotationInFlight = new AtomicBoolean(false);
    private final AtomicBoolean passwordIdRotateInFlight = new AtomicBoolean(false);

    static KeyRotationCoordinator create(
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        FeatureService featureService,
        EncryptionService encryptionService,
        Collection<EncryptedDataHandler<?>> handlers,
        Supplier<Settings> settingsSupplier,
        ProjectEncryptionKeyMetadata.PekEncryption pekEncryption
    ) {
        KeyRotationCoordinator coordinator = new KeyRotationCoordinator(
            clusterService,
            threadPool,
            projectResolver,
            featureService,
            encryptionService,
            handlers,
            settingsSupplier,
            pekEncryption
        );
        clusterService.addLocalNodeMasterListener(coordinator);
        clusterService.addListener(coordinator.clusterStateListener);
        return coordinator;
    }

    KeyRotationCoordinator(
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        FeatureService featureService,
        EncryptionService encryptionService,
        Collection<EncryptedDataHandler<?>> handlers,
        Supplier<Settings> settingsSupplier,
        ProjectEncryptionKeyMetadata.PekEncryption pekEncryption
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.featureService = featureService;
        this.encryptionService = encryptionService;
        this.handlers = new CopyOnWriteArrayList<>(handlers);
        this.taskQueue = clusterService.createTaskQueue(
            "project-encryption-key",
            Priority.NORMAL,
            new KeyRotationExecutor(projectResolver, pekEncryption)
        );
        Settings initialSettings = settingsSupplier.get();
        this.rotationInterval = ROTATION_INTERVAL_SETTING.get(initialSettings);
        this.checkInterval = CHECK_INTERVAL_SETTING.get(initialSettings);
        this.pekEncryption = pekEncryption;
        this.settingsSupplier = settingsSupplier;
    }

    /**
     * Triggers a password-id check on the generic thread pool after a secure-settings reload. This avoids a listener-ordering race where
     * {@link #onClusterStateChanged} fires before {@link EncryptionPlugin#reload} updates the shared settings supplier, causing the
     * mismatch to go undetected until the next periodic tick.
     */
    void reload() {
        threadPool.generic().execute(() -> {
            if (closed) return;
            ClusterState state = clusterService.state();
            if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
                || state.nodes().isLocalNodeElectedMaster() == false) {
                return;
            }
            checkPasswordId(state);
        });
    }

    @Override
    public void onMaster() {
        if (closed) {
            return;
        }
        startSchedule();
    }

    @Override
    public void offMaster() {
        if (closed) {
            return;
        }
        rotating.set(false);
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
        checkPasswordId(state);
    }

    private void checkPasswordId(ClusterState state) {
        var metadata = getCurrentMetadata(state);
        String activePasswordId = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(settingsSupplier.get());
        if (metadata == null) {
            if (activePasswordId != null && checkPekFeatureAvailable(state)) {
                scheduleInstall(activePasswordId);
            }
            return;
        }
        if (activePasswordId != null && activePasswordId.equals(metadata.getPasswordId()) == false) {
            schedulePasswordIdChange(metadata, activePasswordId);
        }
    }

    private void advanceKeyLifecycle(ProjectEncryptionKeyMetadata metadata, long now) {
        if (metadata.isUnwrapFailed()) {
            logger.debug("project encryption key: skipping lifecycle advancement in degraded state");
            return;
        }
        if (rotationDisabled()) {
            return;
        }
        long activeKeyAge = now - metadata.getGeneratedAt(metadata.getActiveKeyId());
        if (activeKeyAge >= rotationInterval.millis()) {
            logger.info("project encryption key due for rotation (active key generated {} ago)", TimeValue.timeValueMillis(activeKeyAge));
            submitBeginRotation();
        }
        long retireCutoff = now - GRACE_TICKS * checkInterval.millis();
        if (metadata.findRetireableKeyIds(retireCutoff).isEmpty() == false) {
            submitRetireKeys(retireCutoff);
        }
    }

    private boolean rotationDisabled() {
        return rotationInterval.duration() == 0;
    }

    private synchronized void startSchedule() {
        if (closed || scheduledTask != null) {
            return;
        }
        logger.debug("starting PEK lifecycle schedule (check_interval={}, rotation_interval={})", checkInterval, rotationInterval);
        scheduledTask = threadPool.scheduleWithFixedDelay(this::tick, checkInterval, threadPool.generic());
    }

    private synchronized void stopSchedule() {
        if (scheduledTask != null) {
            logger.debug("stopping PEK lifecycle schedule");
            scheduledTask.cancel();
            scheduledTask = null;
        }
    }

    void tick() {
        if (closed) {
            return;
        }
        ClusterState state = clusterService.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        checkPasswordId(state);

        ProjectEncryptionKeyMetadata metadata = getCurrentMetadata(state);
        if (metadata == null) {
            return;
        }
        long now = threadPool.absoluteTimeInMillis();
        rotate(metadata, now);
        advanceKeyLifecycle(metadata, now);
    }

    private void rotate(ProjectEncryptionKeyMetadata metadata, long now) {
        String activeKeyId = metadata.getActiveKeyId();
        List<EncryptedDataHandler<?>> pending = handlers.stream().filter(h -> metadata.isHandlerOnActive(h.customName()) == false).toList();
        if (pending.isEmpty()) {
            return;
        }
        if (rotating.compareAndSet(false, true) == false) {
            logger.warn("re-encryption already in progress, skipping this tick");
            return;
        }
        try (
            var listeners = new RefCountingListener(
                ActionListener.runAfter(
                    ActionListener.wrap(unused -> {}, e -> logger.warn("re-encryption failed; will retry on next tick", e)),
                    () -> rotating.set(false)
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
        clusterService.removeListener(clusterStateListener);
        stopSchedule();
    }

    // Visible for testing
    void register(EncryptedDataHandler<?> handler) {
        handlers.add(handler);
        logger.debug("registered encrypted-data handler [{}]", handler.getClass().getSimpleName());
    }

    @Nullable
    ProjectEncryptionKeyMetadata getCurrentMetadata(ClusterState state) {
        return projectResolver.getProjectState(state).metadata().custom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private boolean checkPekFeatureAvailable(ClusterState state) {
        if (featureService.clusterHasFeature(state, ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE) == false) {
            logger.debug("not all nodes support project encryption key feature, waiting for rolling upgrade to complete");
            return false;
        }
        return true;
    }

    private void scheduleInstall(String passwordId) {
        if (installInFlight.compareAndSet(false, true) == false) {
            return;
        }
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                generateAndSubmitInstall(passwordId);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> "failed to submit install-project-encryption-key task for passwordId=" + passwordId, e);
            }

            @Override
            public void onAfter() {
                installInFlight.set(false);
            }
        });
    }

    private void generateAndSubmitInstall(String passwordId) {
        if (ProjectEncryptionKeyPasswordSettings.hasPassword(settingsSupplier.get(), passwordId) == false) {
            logger.warn(
                "project encryption key install skipped: secure setting [{}{}] is not configured",
                ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                passwordId
            );
            return;
        }
        byte[] plaintextPek = randomKey();
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        logger.debug("submitting install-project-encryption-key task: keyId={}, passwordId={}", keyId, passwordId);
        taskQueue.submitTask(
            "install-project-encryption-key",
            new InstallKeyTask(keyId, plaintextPek, passwordId, threadPool.absoluteTimeInMillis()),
            null
        );
    }

    private void submitBeginRotation() {
        if (beginRotationInFlight.compareAndSet(false, true) == false) {
            return;
        }
        // Guard: abort rotation when no active password is configured; nodes must be able to persist the new key to disk.
        try {
            pekEncryption.activePasswordId();
        } catch (Exception e) {
            beginRotationInFlight.set(false);
            logger.warn("project encryption key rotation skipped: no active password configured", e);
            return;
        }
        byte[] plaintextPek = randomKey();
        String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
        taskQueue.submitTask(
            "begin-project-encryption-key-rotation",
            new BeginRotationTask(newKeyId, plaintextPek, threadPool.absoluteTimeInMillis(), beginRotationInFlight),
            null
        );
    }

    private void schedulePasswordIdChange(ProjectEncryptionKeyMetadata metadata, String newPasswordId) {
        if (passwordIdRotateInFlight.compareAndSet(false, true) == false) {
            return;
        }
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            public void doRun() {
                submitPasswordIdChange(metadata, newPasswordId);
            }

            @Override
            public void onAfter() {
                passwordIdRotateInFlight.set(false);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> "failed to submit password id change [" + metadata.getPasswordId() + " -> " + newPasswordId + "]", e);
            }
        });
    }

    private void submitPasswordIdChange(ProjectEncryptionKeyMetadata metadata, String newPasswordId) {
        String oldPasswordId = metadata.getPasswordId();
        Settings settings = this.settingsSupplier.get();
        if (ProjectEncryptionKeyPasswordSettings.hasPassword(settings, oldPasswordId) == false) {
            logger.warn(
                "password id update aborted: previous password [{}{}] is not configured",
                ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                oldPasswordId
            );
            return;
        }
        if (ProjectEncryptionKeyPasswordSettings.hasPassword(settings, newPasswordId) == false) {
            logger.warn(
                "password id update aborted: new password [{}{}] is not configured",
                ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                newPasswordId
            );
            return;
        }
        logger.info("submitting password id change: [{}] -> [{}]", oldPasswordId, newPasswordId);
        taskQueue.submitTask("update-project-encryption-key-password-id", new UpdatePasswordIdTask(newPasswordId, oldPasswordId), null);
    }

    void submitRetireKeys(long cutoffDeactivationMillis) {
        taskQueue.submitTask("retire-project-encryption-keys", new RetireKeysTask(cutoffDeactivationMillis), null);
    }

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        new SecureRandom().nextBytes(keyBytes);
        return keyBytes;
    }

    sealed interface KeyRotationTask extends ClusterStateTaskListener permits InstallKeyTask, BeginRotationTask, RetireKeysTask,
        ReEncryptApplyTask, UpdatePasswordIdTask {

        String description();

        @Override
        default void onFailure(@Nullable Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN, () -> "failure during " + description(), e);
        }
    }

    record InstallKeyTask(String keyId, byte[] pekBytes, String passwordId, long generatedAt) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key initial install";
        }

        @Override
        public String toString() {
            return "InstallKeyTask[keyId=" + keyId + ", passwordId=" + passwordId + ", generatedAt=" + generatedAt + "]";
        }
    }

    record BeginRotationTask(String newKeyId, byte[] pekBytes, long generatedAt, AtomicBoolean inFlight) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key rotation begin";
        }

        @Override
        public void onFailure(@Nullable Exception e) {
            inFlight.set(false);
            KeyRotationTask.super.onFailure(e);
        }

        @Override
        public String toString() {
            return "BeginRotationTask[newKeyId=" + newKeyId + ", generatedAt=" + generatedAt + "]";
        }
    }

    record RetireKeysTask(long cutoffDeactivationMillis) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key retire (cutoff=" + cutoffDeactivationMillis + ")";
        }
    }

    record UpdatePasswordIdTask(String newPasswordId, String expectedOldPasswordId) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key password id update [" + expectedOldPasswordId + " -> " + newPasswordId + "]";
        }
    }

    record ReEncryptApplyTask(
        String customName,
        Metadata.ProjectCustom expectedOld,
        Metadata.ProjectCustom newCustom,
        String expectedActiveKeyId,
        ActionListener<Void> completionListener
    ) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key re-encrypt [" + customName + "]";
        }

        @Override
        public void onFailure(Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN, () -> "failure during " + description(), e);
            completionListener.onFailure(e);
        }
    }

    static class KeyRotationExecutor extends SimpleBatchedExecutor<KeyRotationTask, Void> {

        private final ProjectResolver projectResolver;
        private final ProjectEncryptionKeyMetadata.PekEncryption pekEncryption;

        KeyRotationExecutor(ProjectResolver projectResolver, ProjectEncryptionKeyMetadata.PekEncryption pekEncryption) {
            this.projectResolver = projectResolver;
            this.pekEncryption = pekEncryption;
        }

        @Override
        public Tuple<ClusterState, Void> executeTask(KeyRotationTask task, ClusterState clusterState) {
            ProjectState projectState = projectResolver.getProjectState(clusterState);
            ProjectEncryptionKeyMetadata existing = projectState.metadata().custom(ProjectEncryptionKeyMetadata.TYPE);

            return switch (task) {
                case InstallKeyTask install -> executeInstallInitial(clusterState, projectState, existing, install);
                case BeginRotationTask begin -> executeBeginRotation(clusterState, projectState, existing, begin);
                case RetireKeysTask retireKeysTask -> executeRetireKeys(
                    clusterState,
                    projectState,
                    existing,
                    retireKeysTask.cutoffDeactivationMillis()
                );
                case UpdatePasswordIdTask updatePasswordId -> executeUpdatePasswordId(
                    clusterState,
                    projectState,
                    existing,
                    updatePasswordId
                );
                case ReEncryptApplyTask reEncryptTask -> executeReEncryptApply(clusterState, projectState, existing, reEncryptTask);
            };
        }

        private Tuple<ClusterState, Void> executeInstallInitial(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            InstallKeyTask task
        ) {
            if (existing != null) {
                return Tuple.tuple(clusterState, null);
            }
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
                Map.of(task.keyId(), new ProjectEncryptionKeyMetadata.KeyEntry(task.pekBytes(), task.generatedAt())),
                task.keyId(),
                task.passwordId(),
                Map.of(),
                pekEncryption
            );
            logger.info("installing project encryption key [{}] under password id [{}]", task.keyId(), task.passwordId());
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeBeginRotation(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            BeginRotationTask task
        ) {
            if (existing == null) {
                logger.warn("ignoring begin-rotation task because no project encryption key is installed");
                return Tuple.tuple(clusterState, null);
            }
            Map<String, ProjectEncryptionKeyMetadata.KeyEntry> newEntries = new HashMap<>(existing.getKeys());
            newEntries.put(task.newKeyId(), new ProjectEncryptionKeyMetadata.KeyEntry(task.pekBytes(), task.generatedAt()));
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
                newEntries,
                task.newKeyId(),
                existing.getPasswordId(),
                existing.getHandlerKeyIds(),
                pekEncryption
            );
            logger.info("beginning project encryption key rotation: new active key [{}]", task.newKeyId());
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
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
                retained,
                activeKeyId,
                existing.getPasswordId(),
                existing.getHandlerKeyIds(),
                pekEncryption
            );
            logger.info("project encryption key retire: retained active key [{}], retired keys {}", activeKeyId, new TreeSet<>(retiredIds));
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeUpdatePasswordId(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            UpdatePasswordIdTask task
        ) {
            if (existing == null) {
                logger.debug("dropping password id update task: metadata removed");
                return Tuple.tuple(clusterState, null);
            }
            if (task.newPasswordId().equals(existing.getPasswordId())) {
                return Tuple.tuple(clusterState, null);
            }
            if (existing.getPasswordId().equals(task.expectedOldPasswordId()) == false) {
                logger.debug(
                    "dropping password id update: persisted passwordId [{}] no longer matches expected [{}]",
                    existing.getPasswordId(),
                    task.expectedOldPasswordId()
                );
                return Tuple.tuple(clusterState, null);
            }
            ProjectEncryptionKeyMetadata updated = new ProjectEncryptionKeyMetadata(
                existing.getKeys(),
                existing.getActiveKeyId(),
                task.newPasswordId(),
                existing.getHandlerKeyIds(),
                pekEncryption
            );
            return Tuple.tuple(putMetadata(clusterState, projectState, updated), null);
        }

        private Tuple<ClusterState, Void> executeReEncryptApply(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            ReEncryptApplyTask task
        ) {
            if (existing == null) {
                logger.debug("dropping re-encrypt task for [{}]: no PEK metadata installed", task.customName());
                return Tuple.tuple(clusterState, null);
            }
            if (existing.getActiveKeyId().equals(task.expectedActiveKeyId()) == false) {
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
                logger.debug("dropping re-encrypt task for [{}]: slice mutated between compute and apply", task.customName());
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

        @Override
        public void taskSucceeded(KeyRotationTask task, Void unused) {
            logger.debug("[{}] succeeded", task.description());
            if (task instanceof ReEncryptApplyTask reEncrypt) {
                reEncrypt.completionListener().onResponse(null);
            }
            if (task instanceof BeginRotationTask begin) {
                begin.inFlight().set(false);
            }
        }

        @Override
        public String describeTasks(List<KeyRotationTask> tasks) {
            Map<String, Long> byType = tasks.stream().collect(Collectors.groupingBy(KeyRotationTask::description, Collectors.counting()));
            return "project encryption key tasks " + byType + " [" + tasks.size() + " pending]";
        }
    }
}
