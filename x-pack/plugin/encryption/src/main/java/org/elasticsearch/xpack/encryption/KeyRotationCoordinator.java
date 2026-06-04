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
import org.elasticsearch.common.settings.SecureString;
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
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.Closeable;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Owns the project encryption key (PEK) lifecycle on the elected master: installs the initial key, rotates the active key on a timer,
 * rewraps existing keys when the active password id changes, drives registered {@link EncryptedDataHandler}s to re-encrypt their
 * owned data, and retires non-active keys once their grace window expires.
 *
 * <p>All PEK material is persisted in wrapped form via {@link PasswordBasedEncryption#wrap(byte[], String, char[])}; the
 * password-derivation (PBKDF2) cost is paid here on the generic thread pool — never on the master thread — by pre-computing the wrapped
 * bytes before submitting the cluster-state task.
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
    private final ClusterStateListener clusterStateListener = this::onClusterStateChanged;
    private volatile Settings cachedSettings;

    private volatile Scheduler.Cancellable scheduledTask;
    private volatile boolean closed = false;
    private final AtomicBoolean rotating = new AtomicBoolean(false);
    // Guards async wrap-and-submit work for install / password rotation so we don't spam the master thread with duplicate tasks while
    // earlier ones are still in-flight on the generic pool.
    private final AtomicBoolean installInFlight = new AtomicBoolean(false);
    private final AtomicBoolean passwordRotateInFlight = new AtomicBoolean(false);

    static KeyRotationCoordinator create(
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
            checkInterval,
            settings
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
        TimeValue rotationInterval,
        TimeValue checkInterval,
        Settings settings
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
            new KeyRotationExecutor(projectResolver, threadPool)
        );
        this.rotationInterval = rotationInterval;
        this.checkInterval = checkInterval;
        this.cachedSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings);
    }

    /**
     * Re-binds the coordinator to the password material in {@code settings} (after a secure-settings reload).
     * Immediately schedules a password-id check on the generic thread pool to avoid a listener-ordering race where
     * {@link #onClusterStateChanged} fires before the reload updates {@code cachedSettings}, causing the mismatch
     * to go undetected until the next periodic tick.
     */
    void reload(Settings settings) {
        this.cachedSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings);
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

    /**
     * Checks whether the active password id in {@code cachedSettings} matches the one persisted in {@code metadata}, and schedules an
     * install or password rotation if not.
     */
    private void checkPasswordId(ClusterState state) {
        var metadata = getCurrentMetadata(state);
        String activePasswordId = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(cachedSettings);
        if (metadata == null) {
            // Install runs only if (a) all nodes support PEK, and (b) the operator has configured an active password the master can use
            // to wrap the initial key.
            if (activePasswordId != null && checkPekFeatureAvailable(state)) {
                scheduleInstall(activePasswordId);
            }
            return;
        }
        // Password rotation: same plaintext PEK material, re-wrapped under a new password.
        if (activePasswordId != null && activePasswordId.equals(metadata.getPasswordId()) == false) {
            schedulePasswordRotation(metadata, activePasswordId);
        }
    }

    /**
     * Drives time-based key rotation (begin a new rotation if due) and retirement of expired non-active keys.
     */
    private void advanceKeyLifecycle(ProjectEncryptionKeyMetadata metadata, long now) {
        if (rotationDisabled()) {
            return;
        }
        long activeKeyAge = now - metadata.getGeneratedAt(metadata.getActiveKeyId());
        if (activeKeyAge >= rotationInterval.millis()) {
            logger.info("project encryption key due for rotation (active key generated {} ago)", TimeValue.timeValueMillis(activeKeyAge));
            submitBeginRotation(metadata);
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

    /**
     * Async wrap-and-submit for the initial PEK install. Runs on the generic pool so the PBKDF2 cost is not paid on the master thread.
     */
    private void scheduleInstall(String passwordId) {
        if (installInFlight.compareAndSet(false, true) == false) {
            return;
        }
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                wrapAndSubmitInstall(passwordId);
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

    private void wrapAndSubmitInstall(String passwordId) {
        withPassword(passwordId, "project encryption key install", chars -> {
            byte[] plaintextPek = randomKey();
            try {
                EncryptedData wrapped = PasswordBasedEncryption.wrap(plaintextPek, passwordId, chars);
                String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
                logger.debug("submitting install-project-encryption-key task: keyId={}, passwordId={}", keyId, passwordId);
                taskQueue.submitTask(
                    "install-project-encryption-key",
                    new InstallKeyTask(keyId, wrapped.payload(), passwordId, threadPool.absoluteTimeInMillis()),
                    null
                );
            } finally {
                Arrays.fill(plaintextPek, (byte) 0);
            }
        });
    }

    /**
     * Async wrap-and-submit for {@link BeginRotationTask}. Runs synchronously on the caller's thread (which is the generic pool, since
     * {@link #tick()} is scheduled there) — that is the only place {@link #submitBeginRotation} is called from today.
     */
    private void submitBeginRotation(ProjectEncryptionKeyMetadata existing) {
        String passwordId = existing.getPasswordId();
        withPassword(passwordId, "project encryption key rotation", chars -> {
            byte[] plaintextPek = randomKey();
            try {
                EncryptedData wrapped = PasswordBasedEncryption.wrap(plaintextPek, passwordId, chars);
                String newKeyId = ProjectEncryptionKeyMetadata.generateKeyId();
                taskQueue.submitTask(
                    "begin-project-encryption-key-rotation",
                    new BeginRotationTask(newKeyId, wrapped.payload(), passwordId, threadPool.absoluteTimeInMillis()),
                    null
                );
            } finally {
                Arrays.fill(plaintextPek, (byte) 0);
            }
        });
    }

    /**
     * Resolve the password material for {@code passwordId} and run {@code action} with it. Logs and skips if the setting is not
     * configured. Logs and skips on any exception thrown by {@code action} — the next tick will retry.
     */
    private void withPassword(String passwordId, String operation, Consumer<char[]> action) {
        try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(cachedSettings, passwordId)) {
            if (password == null) {
                logger.warn(
                    "{} skipped: secure setting [{}{}] is not configured",
                    operation,
                    ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                    passwordId
                );
                return;
            }
            action.accept(password.getChars());
        } catch (Exception e) {
            logger.warn(() -> "failed to perform " + operation + " under password id [" + passwordId + "]", e);
        }
    }

    /**
     * Async rewrap-and-submit for password rotation: walks all existing entries, unwraps under the old password, rewraps under the new
     * one, and submits a single batched cluster-state task.
     */
    private void schedulePasswordRotation(ProjectEncryptionKeyMetadata metadata, String newPasswordId) {
        if (passwordRotateInFlight.compareAndSet(false, true) == false) {
            return;
        }
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            public void doRun() {
                rewrapAndSubmit(metadata, newPasswordId);
            }

            @Override
            public void onAfter() {
                passwordRotateInFlight.set(false);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> "failed to rewrap PEK during password rotation [" + metadata.getPasswordId() + " -> " + newPasswordId + "]",
                    e
                );
            }
        });
    }

    private void rewrapAndSubmit(ProjectEncryptionKeyMetadata metadata, String newPasswordId) {
        String oldPasswordId = metadata.getPasswordId();
        var settings = this.cachedSettings;
        try (
            SecureString oldPassword = ProjectEncryptionKeyPasswordSettings.getPassword(settings, oldPasswordId);
            SecureString newPassword = ProjectEncryptionKeyPasswordSettings.getPassword(settings, newPasswordId)
        ) {
            if (oldPassword == null) {
                logger.warn(
                    "password rotation aborted: previous password [{}{}] is no longer configured; cannot unwrap PEK",
                    ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                    oldPasswordId
                );
                return;
            }
            if (newPassword == null) {
                logger.warn(
                    "password rotation aborted: new password [{}{}] is not configured",
                    ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                    newPasswordId
                );
                return;
            }
            Map<String, ProjectEncryptionKeyMetadata.KeyEntry> rewrapped = HashMap.newHashMap(metadata.getKeys().size());
            for (Map.Entry<String, ProjectEncryptionKeyMetadata.KeyEntry> entry : metadata.getKeys().entrySet()) {
                EncryptedData encrypted = new EncryptedData(oldPasswordId, entry.getValue().bytes());
                byte[] plaintext = PasswordBasedEncryption.unwrap(encrypted, oldPassword.getChars());
                try {
                    EncryptedData newEnc = PasswordBasedEncryption.wrap(plaintext, newPasswordId, newPassword.getChars());
                    rewrapped.put(
                        entry.getKey(),
                        new ProjectEncryptionKeyMetadata.KeyEntry(newEnc.payload(), entry.getValue().generatedAt())
                    );
                } finally {
                    Arrays.fill(plaintext, (byte) 0);
                }
            }
            logger.info("submitting password rotation: passwordId [{}] -> [{}]", oldPasswordId, newPasswordId);
            taskQueue.submitTask(
                "rotate-project-encryption-key-password",
                new RotatePasswordTask(Map.copyOf(rewrapped), newPasswordId, oldPasswordId),
                null
            );
        }
    }

    void submitRetireKeys(long cutoffDeactivationMillis) {
        taskQueue.submitTask("retire-project-encryption-keys", new RetireKeysTask(cutoffDeactivationMillis), null);
    }

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        new SecureRandom().nextBytes(keyBytes);
        return keyBytes;
    }

    /**
     * Hierarchy of cluster-state tasks that flow through the PEK task queue. {@link KeyRotationExecutor} dispatches on the concrete type.
     */
    sealed interface KeyRotationTask extends ClusterStateTaskListener permits InstallKeyTask, BeginRotationTask, RetireKeysTask,
        ReEncryptApplyTask, RotatePasswordTask {

        String description();

        @Override
        default void onFailure(@Nullable Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN, () -> "failure during " + description(), e);
        }
    }

    record InstallKeyTask(String keyId, byte[] wrappedBytes, String passwordId, long generatedAt) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key initial install";
        }
    }

    record BeginRotationTask(String newKeyId, byte[] wrappedBytes, String passwordId, long generatedAt) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key rotation begin";
        }
    }

    record RetireKeysTask(long cutoffDeactivationMillis) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key retire (cutoff=" + cutoffDeactivationMillis + ")";
        }
    }

    record RotatePasswordTask(
        Map<String, ProjectEncryptionKeyMetadata.KeyEntry> rewrappedEntries,
        String newPasswordId,
        String expectedOldPasswordId
    ) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key password rotate [" + expectedOldPasswordId + " -> " + newPasswordId + "]";
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
        private final ThreadPool threadPool;

        KeyRotationExecutor(ProjectResolver projectResolver, ThreadPool threadPool) {
            this.projectResolver = projectResolver;
            this.threadPool = threadPool;
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
                case RotatePasswordTask rotatePassword -> executeRotatePassword(clusterState, projectState, existing, rotatePassword);
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
                // Initial install is idempotent: if metadata already exists, leave it alone.
                return Tuple.tuple(clusterState, null);
            }
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
                Map.of(task.keyId(), new ProjectEncryptionKeyMetadata.KeyEntry(task.wrappedBytes(), task.generatedAt())),
                task.keyId(),
                task.passwordId()
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
            // Don't apply if the persisted passwordId differs from what we wrapped under. The next tick will produce a fresh wrap
            if (existing.getPasswordId().equals(task.passwordId()) == false) {
                logger.debug(
                    "dropping begin-rotation task: persisted passwordId [{}] differs from task passwordId [{}]",
                    existing.getPasswordId(),
                    task.passwordId()
                );
                return Tuple.tuple(clusterState, null);
            }
            Map<String, ProjectEncryptionKeyMetadata.KeyEntry> newEntries = new HashMap<>(existing.getKeys());
            newEntries.put(task.newKeyId(), new ProjectEncryptionKeyMetadata.KeyEntry(task.wrappedBytes(), task.generatedAt()));
            ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
                newEntries,
                task.newKeyId(),
                existing.getPasswordId(),
                existing.getHandlerKeyIds()
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
                existing.getHandlerKeyIds()
            );
            logger.info("project encryption key retire: retained active key [{}], retired keys {}", activeKeyId, new TreeSet<>(retiredIds));
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
        }

        private Tuple<ClusterState, Void> executeRotatePassword(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            RotatePasswordTask task
        ) {
            if (existing == null) {
                logger.debug("dropping password rotation task: metadata removed");
                return Tuple.tuple(clusterState, null);
            }
            if (task.newPasswordId().equals(existing.getPasswordId())) {
                return Tuple.tuple(clusterState, null);
            }
            if (existing.getPasswordId().equals(task.expectedOldPasswordId()) == false) {
                logger.debug(
                    "dropping password rotation: persisted passwordId [{}] no longer matches rewrap source [{}]",
                    existing.getPasswordId(),
                    task.expectedOldPasswordId()
                );
                return Tuple.tuple(clusterState, null);
            }

            if (existing.getKeys().keySet().equals(task.rewrappedEntries().keySet()) == false) {
                logger.debug("dropping password rotation: key set changed between rewrap and apply");
                return Tuple.tuple(clusterState, null);
            }
            ProjectEncryptionKeyMetadata updated = new ProjectEncryptionKeyMetadata(
                task.rewrappedEntries(),
                existing.getActiveKeyId(),
                task.newPasswordId(),
                existing.getHandlerKeyIds()
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
        }

        @Override
        public String describeTasks(List<KeyRotationTask> tasks) {
            Map<String, Long> byType = tasks.stream().collect(Collectors.groupingBy(KeyRotationTask::description, Collectors.counting()));
            return "project encryption key tasks " + byType + " [" + tasks.size() + " pending]";
        }
    }
}
