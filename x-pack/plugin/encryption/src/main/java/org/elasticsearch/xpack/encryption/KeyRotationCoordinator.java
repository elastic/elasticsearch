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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
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
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Owns the project encryption key (PEK) lifecycle on the elected master.
 * <ul>
 *   <li><b>Install</b>: with no key and an active password configured, the master asks every node that persists cluster state whether
 *   it can wrap the key ({@link TransportNodesCanPersistKeyAction}) and installs one only once all can. {@link InstallKeyTask} is dropped
 *   if such a node joined since the check.</li>
 *   <li><b>Persist check retry</b>: while a node can't, the master asks again after {@link #PERSIST_CHECK_INTERVAL}, or right
 *   away when it reloads its secure settings or a node that persists cluster state joins or leaves.</li>
 *   <li><b>Persist check recheck</b>: an install or restore requested while a check is in flight, e.g. by a join, runs once that check
 *   completes.</li>
 *   <li><b>Join condition</b>: once the key is installed, a node that persists cluster state but can't wrap the key can't join
 *   ({@link EncryptionPlugin#getJoinValidator()}).</li>
 *   <li><b>Key rotation</b>: once the active key is older than {@link #ROTATION_INTERVAL_SETTING}, a new key becomes active, the
 *   registered {@link EncryptedDataHandler}s re-encrypt their data under it, and keys no longer in use are retired.</li>
 *   <li><b>Password rotation</b>: when the active password id changes, {@link ProjectEncryptionKeyMetadata#getPasswordId()} follows.</li>
 *   <li><b>Restore</b>: a master that can't read the key on its disk after a restart holds no key
 *   ({@link ProjectEncryptionKeyMetadata#isUnwrapFailed()}) and drops every other task. It reads the key from its disk once a secure
 *   settings reload gives it the password, and restores it, like the install, only once every node that persists cluster state can
 *   wrap it.</li>
 *   <li><b>Reset</b>: on every node, not only the master, forgets this node's copy of a key that a reset removed.</li>
 * </ul>
 *
 * <p>Re-encryption uses a compute-then-CAS pattern: each handler re-encrypts against a cluster-state snapshot on the generic pool, then
 * a {@link ReEncryptApplyTask} atomically applies the result — aborting if the snapshot is stale. Conflicts retry on the next tick.
 */
class KeyRotationCoordinator implements LocalNodeMasterListener, Closeable {

    private static final Logger logger = LogManager.getLogger(KeyRotationCoordinator.class);

    // Number of check_intervals a non-active key persists after its deactivation time before retirement.
    private static final int GRACE_TICKS = 10;

    // how long the master waits for nodes to answer whether they can persist the key, and before asking again while some can't
    static final TimeValue PERSIST_CHECK_INTERVAL = TimeValue.timeValueSeconds(10);

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
    private final Client client;
    private final boolean stateless;

    private volatile Scheduler.Cancellable scheduledTask;
    private volatile boolean closed = false;
    private final AtomicBoolean reEncrypting = new AtomicBoolean(false);
    // held from the check that every node can persist the key until the install or restore is submitted or, if a node is not ready,
    // until the retry
    private final AtomicBoolean persistCheckInFlight = new AtomicBoolean(false);
    private final AtomicBoolean beginRotationInFlight = new AtomicBoolean(false);
    private final AtomicBoolean passwordIdRotateInFlight = new AtomicBoolean(false);
    // set by a call that found a check in flight; once that check succeeds, the master reconciles again
    private final AtomicBoolean persistCheckRecheck = new AtomicBoolean(false);
    // set while a retry of that check is pending; whichever of the scheduled retry and an early one takes it releases the check
    private final AtomicReference<Object> pendingPersistCheckRetry = new AtomicReference<>();
    // node names in the last "cannot persist it" warning, so it is logged only when they change
    private final AtomicReference<Set<String>> nodesWithoutPEKPassword = new AtomicReference<>();
    // likewise for the nodes that didn't answer the check
    private final AtomicReference<Set<String>> nodesNotAnswering = new AtomicReference<>();
    // one restore attempt per secure settings snapshot while master, since only a reload can make this node's copy readable
    private final AtomicReference<Settings> lastRestoreAttempt = new AtomicReference<>();
    // the keys this master unwrapped from its copy on disk, restored once every node that persists cluster state can wrap them
    private final AtomicReference<ProjectEncryptionKeyMetadata> keysToRestore = new AtomicReference<>();

    static KeyRotationCoordinator create(
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        FeatureService featureService,
        EncryptionService encryptionService,
        Collection<EncryptedDataHandler<?>> handlers,
        Supplier<Settings> settingsSupplier,
        ProjectEncryptionKeyMetadata.PekEncryption pekEncryption,
        Client client
    ) {
        KeyRotationCoordinator coordinator = new KeyRotationCoordinator(
            clusterService,
            threadPool,
            projectResolver,
            featureService,
            encryptionService,
            handlers,
            settingsSupplier,
            pekEncryption,
            client
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
        ProjectEncryptionKeyMetadata.PekEncryption pekEncryption,
        Client client
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.featureService = featureService;
        this.encryptionService = encryptionService;
        this.handlers = new CopyOnWriteArrayList<>(handlers);
        this.stateless = DiscoveryNode.isStateless(clusterService.getSettings());
        this.taskQueue = clusterService.createTaskQueue(
            "project-encryption-key",
            Priority.NORMAL,
            new KeyRotationExecutor(projectResolver, pekEncryption, stateless)
        );
        Settings initialSettings = settingsSupplier.get();
        this.rotationInterval = ROTATION_INTERVAL_SETTING.get(initialSettings);
        this.checkInterval = CHECK_INTERVAL_SETTING.get(initialSettings);
        this.pekEncryption = pekEncryption;
        this.settingsSupplier = settingsSupplier;
    }

    /**
     * Reconciles the key with the reloaded secure settings on the generic thread pool. This avoids a listener-ordering race where
     * {@link #onClusterStateChanged} fires before {@link EncryptionPlugin#reload} updates the shared settings supplier, causing the
     * mismatch to go undetected until the next periodic tick.
     */
    void reload() {
        threadPool.generic().execute(() -> {
            Object retry = pendingPersistCheckRetry.get();
            if (retry != null) {
                runPersistCheckRetry(retry);
            } else {
                reconcileKeyIfMaster();
            }
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
        reEncrypting.set(false);
        lastRestoreAttempt.set(null);
        keysToRestore.set(null);
        nodesWithoutPEKPassword.set(null);
        nodesNotAnswering.set(null);
        stopSchedule();
    }

    // package-private for testing
    void onClusterStateChanged(ClusterChangedEvent event) {
        if (closed) {
            return;
        }
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        // on every node, not only the master
        forgetCopyIfKeyWasReset(event);
        if (event.localNodeMaster() == false) {
            return;
        }
        Object retry = pendingPersistCheckRetry.get();
        // a node that needs a password joined or left, which may unblock the waiting install or restore
        if (retry != null && nodesThatNeedPasswordChanged(event)) {
            threadPool.generic().execute(() -> runPersistCheckRetry(retry));
            return;
        }
        reconcileKey(state);
    }

    private void forgetCopyIfKeyWasReset(ClusterChangedEvent event) {
        if (event.previousState().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
            ProjectEncryptionKeyMetadata previous = getCurrentMetadata(event.previousState());
            if (previous != null && getCurrentMetadata(event.state()) == null) {
                pekEncryption.gatewayKeys().forgetCopyOf(previous.getActiveKeyId());
            }
        }
    }

    private boolean nodesThatNeedPasswordChanged(ClusterChangedEvent event) {
        return event.nodesChanged()
            && Stream.concat(event.nodesDelta().addedNodes().stream(), event.nodesDelta().removedNodes().stream())
                .anyMatch(node -> PasswordPekEncryption.persistsClusterState(node.getRoles(), stateless));
    }

    /** Installs the first key, restores a key the master can't read, or follows a changed active password id, whichever applies. */
    private void reconcileKey(ClusterState state) {
        var metadata = getCurrentMetadata(state);
        String activePasswordId = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(settingsSupplier.get());
        // First install
        if (metadata == null) {
            if (activePasswordId != null
                && featureService.clusterHasFeature(state, ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE)) {
                scheduleInstall(activePasswordId, state);
            }
            return;
        }
        // Previous failed unwrap of encrypted PEK
        if (metadata.isUnwrapFailed()) {
            scheduleRestore(metadata, state);
            return;
        }
        keysToRestore.set(null);
        nodesWithoutPEKPassword.set(null);
        nodesNotAnswering.set(null);
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
            submitTaskIfOpen("retire-project-encryption-keys", new RetireKeysTask(retireCutoff, metadata.getActiveKeyId()));
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
        reconcileKey(state);

        ProjectEncryptionKeyMetadata metadata = getCurrentMetadata(state);
        if (metadata == null || metadata.isUnwrapFailed()) {
            return;
        }
        long now = threadPool.absoluteTimeInMillis();
        var reEncryptionInProgress = reEncrypt(metadata);
        // In-flight ReEncryptApplyTasks would lose their CAS check if the active key rotated now.
        if (reEncryptionInProgress) {
            logger.debug("project encryption key: deferring lifecycle advancement; re-encryption still in progress");
            return;
        }

        advanceKeyLifecycle(metadata, now);
    }

    private boolean reEncrypt(ProjectEncryptionKeyMetadata metadata) {
        String activeKeyId = metadata.getActiveKeyId();
        List<EncryptedDataHandler<?>> pending = handlers.stream().filter(h -> metadata.isHandlerOnActive(h.customName()) == false).toList();
        if (pending.isEmpty()) {
            return false;
        }
        if (reEncrypting.compareAndSet(false, true) == false) {
            logger.warn("re-encryption already in progress, skipping this tick");
            return true;
        }
        try (
            var listeners = new RefCountingListener(
                ActionListener.runAfter(
                    ActionListener.wrap(unused -> {}, e -> logger.warn("re-encryption failed; will retry on next tick", e)),
                    () -> reEncrypting.set(false)
                )
            )
        ) {
            for (EncryptedDataHandler<?> handler : pending) {
                ActionListener<Void> l = listeners.acquire();
                threadPool.generic().execute(() -> dispatchOne(handler, activeKeyId, l));
            }
        }
        return true;
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
            UnaryOperator<EncryptedData> rewrapper = existing -> {
                if (existing.keyId().equals(expectedActiveKeyId)) {
                    return existing;
                }
                byte[] plaintext = encryptionService.decrypt(existing);
                try {
                    return encryptionService.encrypt(plaintext);
                } finally {
                    Arrays.fill(plaintext, (byte) 0);
                }
            };
            T newCustom = handler.reEncrypt(oldCustom, rewrapper);
            if (oldCustom != null && newCustom == null) {
                listener.onFailure(new IllegalStateException("Re-encrypting non-null state returned null state"));
                return;
            }

            assert newCustom == null || handler.customName().equals(newCustom.getWriteableName())
                : "handler ["
                    + handler.getClass().getSimpleName()
                    + "] customName="
                    + handler.customName()
                    + " does not match returned custom getWriteableName="
                    + newCustom.getWriteableName();
            var task = new ReEncryptApplyTask(handler.customName(), oldCustom, newCustom, expectedActiveKeyId, listener);
            if (submitTaskIfOpen("re-encrypt-" + handler.customName(), task) == false) {
                // we still need to complete the callback when the cluster is shutting down, otherwise we'll end up with a dangling listener
                listener.onResponse(null);
            }
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

    private synchronized boolean submitTaskIfOpen(String source, KeyRotationTask task) {
        if (closed) {
            // do not submit new tasks when cluster is shutting down
            return false;
        }
        taskQueue.submitTask(source, task, null);
        return true;
    }

    /**
     * Returns the names of the nodes that the elected master last found unable to persist the key while an install or restore waits
     * for them. Empty on other nodes, and once the key is installed or restored.
     */
    Set<String> nodesWithoutPassword() {
        Set<String> nodes = nodesWithoutPEKPassword.get();
        // a check that completes after this node stopped being master would leave its names behind
        return nodes == null || clusterService.state().nodes().isLocalNodeElectedMaster() == false ? Set.of() : nodes;
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

    private void scheduleInstall(String passwordId, ClusterState state) {
        if (ProjectEncryptionKeyPasswordSettings.hasPassword(settingsSupplier.get(), passwordId) == false) {
            logger.warn(
                "project encryption key install skipped: secure setting [{}{}] is not configured",
                ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                passwordId
            );
            return;
        }
        whenEveryNodeCanPersist(state, "installed", ready -> submitInstall(passwordId, ready));
    }

    /**
     * Calls {@code onReady} with the ids of the nodes that reported they can wrap the key, once every node that persists cluster state
     * has, and asks again while some can't. One check runs at a time. {@code operation} is what waits for it, as a past participle for
     * the warning ("installed" or "restored"). When every node gets the password from cluster secrets, checks only this node and calls
     * it with {@code null}. A call made while a check runs doesn't call its {@code onReady}; the master reconciles again once that check
     * completes.
     */
    private void whenEveryNodeCanPersist(ClusterState state, String operation, Consumer<Set<String>> onReady) {
        if (PasswordPekEncryption.passwordsFromClusterSecrets(state, stateless)) {
            // every node has the master's password, which can still be unusable, such as one too short for FIPS
            threadPool.generic().execute(() -> {
                String failure = pekEncryption.wrapForGatewayFailure();
                if (failure != null) {
                    Set<String> self = Set.of(state.nodes().getLocalNode().getName());
                    if (self.equals(nodesWithoutPEKPassword.getAndSet(self)) == false) {
                        logger.warn("project encryption key not {} yet: the elected master cannot persist it ({})", operation, failure);
                    }
                    return;
                }
                nodesWithoutPEKPassword.set(null);
                onReady.accept(null);
            });
            return;
        }
        // If a node joined during the check, the install or restore would be dropped (not all nodes are guaranteed to be able to wrap the
        // PEK), so check again once this one completes. Set before acquiring, so a holder that releases concurrently still sees it.
        persistCheckRecheck.set(true);
        if (persistCheckInFlight.compareAndSet(false, true) == false) {
            return;
        }
        persistCheckRecheck.set(false);
        DiscoveryNode[] nodes = state.nodes()
            .getAllNodes()
            .stream()
            .filter(node -> PasswordPekEncryption.persistsClusterState(node.getRoles(), stateless))
            .toArray(DiscoveryNode[]::new);

        ActionListener<TransportNodesCanPersistKeyAction.Response> listener = ActionListener.wrap(
            response -> onPersistCheckResponse(state, response, operation, onReady),
            e -> {
                logger.warn("failed to check whether every node can persist the project encryption key", e);
                schedulePersistCheckRetry();
            }
        );
        // forked to avoid calling on the cluster applier thread
        threadPool.generic().execute(ActionRunnable.wrap(listener, l -> {
            var request = new TransportNodesCanPersistKeyAction.Request(nodes);
            request.setTimeout(PERSIST_CHECK_INTERVAL);
            try (var ignored = threadPool.getThreadContext().newEmptySystemContext()) {
                client.execute(TransportNodesCanPersistKeyAction.TYPE, request, l);
            }
        }));
    }

    private void onPersistCheckResponse(
        ClusterState state,
        TransportNodesCanPersistKeyAction.Response response,
        String operation,
        Consumer<Set<String>> onReady
    ) {
        Set<String> ready = response.readyNodeIds();
        if (everyPersistingNodeIsReady(state, ready, stateless) == false) {
            warnAboutNodesThatCannotPersist(state, response, operation);
            schedulePersistCheckRetry();
            return;
        }
        nodesWithoutPEKPassword.set(null);
        nodesNotAnswering.set(null);
        try {
            onReady.accept(ready);
        } catch (Exception e) {
            // not rethrown: the failure handler would schedule a retry that releases a check another call may hold by then
            logger.warn(() -> "failed to submit the project encryption key task once every node could persist it", e);
        } finally {
            persistCheckInFlight.set(false);
        }
        if (persistCheckRecheck.getAndSet(false)) {
            reconcileKeyIfMaster();
        }
    }

    private void warnAboutNodesThatCannotPersist(
        ClusterState state,
        TransportNodesCanPersistKeyAction.Response response,
        String operation
    ) {
        Set<String> notReady = response.getNodes()
            .stream()
            .filter(r -> r.canPersist() == false)
            .map(r -> r.getNode().getName())
            .collect(Collectors.toCollection(TreeSet::new));
        Set<String> previouslyNotReady = this.nodesWithoutPEKPassword.getAndSet(notReady.isEmpty() ? null : notReady);
        if (notReady.isEmpty() == false && notReady.equals(previouslyNotReady) == false) {
            logger.warn(
                "project encryption key not {} yet: nodes {} cannot persist it. Configure [{}] and the matching [{}<id>]"
                    + " secure setting on these nodes and reload their secure settings",
                operation,
                notReady,
                ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY,
                ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
            );
        }
        Set<String> notAnswering = response.failures().stream().map(f -> {
            DiscoveryNode node = state.nodes().get(f.nodeId());
            return node == null ? f.nodeId() : node.getName();
        }).collect(Collectors.toCollection(TreeSet::new));
        Set<String> previouslyNotAnswering = nodesNotAnswering.getAndSet(notAnswering.isEmpty() ? null : notAnswering);
        if (notAnswering.isEmpty() == false && notAnswering.equals(previouslyNotAnswering) == false) {
            logger.warn(
                "project encryption key not {} yet: nodes {} did not report whether they can persist it. Check that they are reachable"
                    + " from the elected master and see their logs",
                operation,
                notAnswering
            );
        }
        if (response.hasFailures()) {
            logger.debug(
                "project encryption key not {} yet: nodes did not report whether they can persist it {}",
                operation,
                response.failures()
            );
        }
    }

    /** Returns whether every node in {@code state} that persists cluster state is among {@code readyNodeIds}, by ephemeral id. */
    static boolean everyPersistingNodeIsReady(ClusterState state, Set<String> readyNodeIds, boolean stateless) {
        return state.nodes()
            .getAllNodes()
            .stream()
            .noneMatch(
                node -> PasswordPekEncryption.persistsClusterState(node.getRoles(), stateless)
                    && readyNodeIds.contains(node.getEphemeralId()) == false
            );
    }

    private void releasePersistCheck() {
        persistCheckInFlight.set(false);
        persistCheckRecheck.set(false);
    }

    private void submitInstall(String passwordId, @Nullable Set<String> readyNodeIds) {
        byte[] plaintextPek = randomKey();
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        logger.debug("submitting install-project-encryption-key task: keyId={}, passwordId={}", keyId, passwordId);
        submitTaskIfOpen(
            "install-project-encryption-key",
            new InstallKeyTask(keyId, plaintextPek, passwordId, threadPool.absoluteTimeInMillis(), readyNodeIds)
        );
    }

    // nodes don't tell the master when they reload secure settings, so it has to ask again
    private void schedulePersistCheckRetry() {
        Object retry = new Object();
        pendingPersistCheckRetry.set(retry);
        try {
            threadPool.schedule(() -> runPersistCheckRetry(retry), PERSIST_CHECK_INTERVAL, threadPool.generic());
        } catch (Exception e) {
            pendingPersistCheckRetry.set(null);
            releasePersistCheck();
            logger.debug("could not schedule a retry of the project encryption key persist check", e);
        }
    }

    private void runPersistCheckRetry(Object retry) {
        if (pendingPersistCheckRetry.compareAndSet(retry, null)) {
            releasePersistCheck();
            reconcileKeyIfMaster();
        }
    }

    private void reconcileKeyIfMaster() {
        if (closed == false) {
            ClusterState state = clusterService.state();
            if (state.nodes().isLocalNodeElectedMaster()
                && state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                reconcileKey(state);
            }
        }
    }

    /**
     * Restores the key while this master holds none. Once it has unwrapped its copy of the key on disk, waits for every node that
     * persists cluster state and submits {@link RestoreKeysTask}; until then, tries to unwrap it once per secure settings snapshot, since
     * only a reload can make its copy readable.
     */
    private void scheduleRestore(ProjectEncryptionKeyMetadata degraded, ClusterState state) {
        String activeKeyId = degraded.getActiveKeyId();
        ProjectEncryptionKeyMetadata keys = keysToRestore.get();
        if (keys != null && Objects.equals(keys.getActiveKeyId(), activeKeyId)) {
            whenEveryNodeCanPersist(
                state,
                "restored",
                ready -> submitTaskIfOpen("restore-project-encryption-keys", new RestoreKeysTask(keys, ready))
            );
            return;
        }
        Settings settings = settingsSupplier.get();
        // No settings changed so nothing to restore
        if (settings == lastRestoreAttempt.getAndSet(settings)) {
            return;
        }
        if (ProjectEncryptionKeyPasswordSettings.hasPassword(
            settings,
            ProjectEncryptionKeyPasswordSettings.getActivePasswordId(settings)
        )) {
            unwrapGatewayKeys(activeKeyId);
        } else {
            logger.warn(
                "the elected master holds no project encryption key [{}] and cannot persist it, so cluster state encryption is unavailable."
                    + " Configure [{}] and the matching [{}<id>] secure setting on this node and reload secure settings",
                activeKeyId,
                ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY,
                ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
            );
        }
    }

    private void unwrapGatewayKeys(String activeKeyId) {
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                ProjectEncryptionKeyMetadata keys = ProjectEncryptionKeyMetadata.unwrapGatewayKeys(activeKeyId, pekEncryption);
                if (keys == null) {
                    throw new IllegalStateException("this node has no copy of it on disk");
                }
                logger.info(
                    "unwrapped project encryption key [{}] from this node's disk, restoring it once every node can persist it",
                    activeKeyId
                );
                keysToRestore.set(keys);
                reconcileKeyIfMaster();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> Strings.format(
                        "the elected master holds no project encryption key [%s] and cannot read it from its disk, so cluster state"
                            + " encryption is unavailable. Configure the password the key on its disk is wrapped with and reload secure"
                            + " settings, or fail the master over to a master-eligible node that can read its copy, for example by"
                            + " excluding this node from the voting configuration. If no node can, call"
                            + " POST /_encryption/_reset?accept_data_loss=true, which deletes all encrypted data",
                        activeKeyId
                    ),
                    e
                );
            }
        });
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
        var task = new BeginRotationTask(newKeyId, plaintextPek, threadPool.absoluteTimeInMillis(), beginRotationInFlight);
        if (submitTaskIfOpen("begin-project-encryption-key-rotation", task) == false) {
            beginRotationInFlight.set(false);
        }
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
        if (ProjectEncryptionKeyPasswordSettings.hasPassword(settings, newPasswordId) == false) {
            logger.warn(
                "password id update aborted: new password [{}{}] is not configured",
                ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                newPasswordId
            );
            return;
        }
        logger.info("submitting password id change: [{}] -> [{}]", oldPasswordId, newPasswordId);
        submitTaskIfOpen("update-project-encryption-key-password-id", new UpdatePasswordIdTask(newPasswordId, oldPasswordId));
    }

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        new SecureRandom().nextBytes(keyBytes);
        return keyBytes;
    }

    sealed interface KeyRotationTask extends ClusterStateTaskListener permits InstallKeyTask, BeginRotationTask, RetireKeysTask,
        ReEncryptApplyTask, UpdatePasswordIdTask, RestoreKeysTask {

        String description();

        @Override
        default void onFailure(@Nullable Exception e) {
            logger.log(MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN, () -> "failure during " + description(), e);
        }
    }

    /**
     * Installs the first key, but only if every node that persists cluster state is among {@code readyNodeIds}, the ephemeral ids of the
     * nodes that reported they can persist the key. {@code null} when the passwords come from cluster secrets and only the master is
     * checked.
     */
    record InstallKeyTask(String keyId, byte[] pekBytes, String passwordId, long generatedAt, @Nullable Set<String> readyNodeIds)
        implements
            KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key initial install";
        }

        @Override
        public String toString() {
            return "InstallKeyTask[keyId=" + keyId + ", passwordId=" + passwordId + ", generatedAt=" + generatedAt + "]";
        }
    }

    /**
     * Replaces the key-less metadata an elected master published with the keys it read from its own disk for the same active key, but
     * only if every node that persists cluster state is among {@code readyNodeIds}, as for {@link InstallKeyTask}.
     */
    record RestoreKeysTask(ProjectEncryptionKeyMetadata keys, @Nullable Set<String> readyNodeIds) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key restore [" + keys.getActiveKeyId() + "]";
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

    record RetireKeysTask(long cutoffDeactivationMillis, String expectedActiveKeyId) implements KeyRotationTask {
        @Override
        public String description() {
            return "project encryption key retire (cutoff="
                + cutoffDeactivationMillis
                + ", expectedActiveKeyId="
                + expectedActiveKeyId
                + ")";
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
        @Nullable Metadata.ProjectCustom newCustom,
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
        private final boolean stateless;

        KeyRotationExecutor(ProjectResolver projectResolver, ProjectEncryptionKeyMetadata.PekEncryption pekEncryption, boolean stateless) {
            this.projectResolver = projectResolver;
            this.pekEncryption = pekEncryption;
            this.stateless = stateless;
        }

        @Override
        public Tuple<ClusterState, Void> executeTask(KeyRotationTask task, ClusterState clusterState) {
            ProjectState projectState = projectResolver.getProjectState(clusterState);
            ProjectEncryptionKeyMetadata existing = projectState.metadata().custom(ProjectEncryptionKeyMetadata.TYPE);
            // while the master holds no key, any other task would publish metadata without the keys still on the nodes' disks
            if (existing != null && existing.isUnwrapFailed() && task instanceof RestoreKeysTask == false) {
                logger.debug("dropping [{}]: the elected master holds no project encryption key", task.description());
                return Tuple.tuple(clusterState, null);
            }

            return switch (task) {
                case InstallKeyTask install -> executeInstallInitial(clusterState, projectState, existing, install);
                case RestoreKeysTask restore -> executeRestoreKeys(clusterState, projectState, existing, restore);
                case BeginRotationTask begin -> executeBeginRotation(clusterState, projectState, existing, begin);
                case RetireKeysTask retireKeysTask -> executeRetireKeys(clusterState, projectState, existing, retireKeysTask);
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
            if (notEveryNodeIsReady(task, clusterState, task.readyNodeIds())) {
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

        private Tuple<ClusterState, Void> executeRestoreKeys(
            ClusterState clusterState,
            ProjectState projectState,
            ProjectEncryptionKeyMetadata existing,
            RestoreKeysTask task
        ) {
            // never undo a reset or replace a newer key: only replace key-less metadata for the same active key
            if (existing == null
                || existing.isUnwrapFailed() == false
                || Objects.equals(task.keys().getActiveKeyId(), existing.getActiveKeyId()) == false) {
                logger.debug("dropping restore task: project encryption key metadata changed to [{}]", existing);
                return Tuple.tuple(clusterState, null);
            }
            if (notEveryNodeIsReady(task, clusterState, task.readyNodeIds())) {
                return Tuple.tuple(clusterState, null);
            }
            logger.info("restored project encryption key [{}]", task.keys().getActiveKeyId());
            return Tuple.tuple(putMetadata(clusterState, projectState, task.keys()), null);
        }

        private boolean notEveryNodeIsReady(KeyRotationTask task, ClusterState clusterState, @Nullable Set<String> readyNodeIds) {
            if (readyNodeIds == null) {
                if (PasswordPekEncryption.passwordsFromClusterSecrets(clusterState, stateless)) {
                    return false;
                }
                logger.debug("dropping [{}]: the passwords no longer come from cluster secrets", task.description());
                return true;
            }
            if (everyPersistingNodeIsReady(clusterState, readyNodeIds, stateless)) {
                return false;
            }
            logger.debug("dropping [{}]: a node joined since the readiness check", task.description());
            return true;
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
            RetireKeysTask task
        ) {
            if (existing == null) {
                return Tuple.tuple(clusterState, null);
            }
            // a rotation since the tick makes handlers pending again; retiring now would skip their pass over the new active key
            if (existing.getActiveKeyId().equals(task.expectedActiveKeyId()) == false) {
                logger.debug(
                    "dropping retire task: activeKeyId drifted from [{}] to [{}]",
                    task.expectedActiveKeyId(),
                    existing.getActiveKeyId()
                );
                return Tuple.tuple(clusterState, null);
            }
            Set<String> retiredIds = existing.findRetireableKeyIds(task.cutoffDeactivationMillis());
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
            ClusterState newState = clusterState.copyAndUpdateProject(projectState.projectId(), b -> {
                if (task.newCustom() != null) {
                    b = b.putCustom(task.customName(), task.newCustom());
                }
                b.putCustom(ProjectEncryptionKeyMetadata.TYPE, updatedPek);
            });
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
