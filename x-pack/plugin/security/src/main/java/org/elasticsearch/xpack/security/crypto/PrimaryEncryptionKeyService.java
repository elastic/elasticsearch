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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.crypto.KeyRotationHandler;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;

import java.security.SecureRandom;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.crypto.SecretKey;

/**
 * Generates and distributes the primary encryption key via cluster state. Owns the key material in project metadata and publishes the
 * cluster-state transitions that install, rotate, and retire it.
 *
 * <p>On master election, generates an AES-256 key if none exists. Subsequent rotations, driven by {@link KeyRotationCoordinator}, flow
 * through the same {@link MasterServiceTaskQueue}: {@link BeginRotationTask} adds a fresh active key alongside any existing keys;
 * {@link RetireKeysTask} drops non-active keys older than a caller-supplied cutoff.
 */
public class PrimaryEncryptionKeyService implements AesGcmEncryptionService.KeyProvider {

    private static final Logger logger = LogManager.getLogger(PrimaryEncryptionKeyService.class);

    public static final FeatureFlag PRIMARY_ENCRYPTION_KEY_FEATURE_FLAG = new FeatureFlag("primary_encryption_key");

    // Prevents key generation in a mixed-version cluster. Without this, TransportVersion filtering
    // would omit the PEK custom from cluster state sent to old nodes (they lack the transport version
    // for it). If an old node becomes master, it sees no existing key and generates a new one,
    // orphaning any data encrypted with the original key.
    public static final NodeFeature PRIMARY_ENCRYPTION_KEY_FEATURE = new NodeFeature("security.primary_encryption_key");

    /**
     * How frequently the primary encryption key is rotated. {@link TimeValue#ZERO} disables rotation.
     */
    public static final Setting<TimeValue> ROTATION_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.security.encryption.key_rotation.interval",
        TimeValue.timeValueDays(30),
        TimeValue.ZERO,
        Property.NodeScope
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
        Property.NodeScope
    );

    private final MasterServiceTaskQueue<KeyRotationTask> taskQueue;
    private final ProjectResolver projectResolver;
    private final FeatureService featureService;
    private final Map<String, KeyRotationHandler> rotationHandlers = new ConcurrentHashMap<>();
    private volatile KeyCache cache = KeyCache.EMPTY;

    private PrimaryEncryptionKeyService(
        MasterServiceTaskQueue<KeyRotationTask> taskQueue,
        ProjectResolver projectResolver,
        FeatureService featureService
    ) {
        this.taskQueue = taskQueue;
        this.projectResolver = projectResolver;
        this.featureService = featureService;
    }

    public static PrimaryEncryptionKeyService create(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        FeatureService featureService
    ) {
        PrimaryEncryptionKeyService service = new PrimaryEncryptionKeyService(
            clusterService.createTaskQueue(
                "primary-encryption-key",
                Priority.NORMAL,
                new KeyRotationExecutor(projectResolver, clusterService.threadPool())
            ),
            projectResolver,
            featureService
        );
        clusterService.addListener(service::onClusterStateChanged);
        return service;
    }

    private void onClusterStateChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();

        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("primary encryption key: cluster not recovered yet, skipping");
            return;
        }

        if (event.changedCustomProjectMetadataSet().contains(PrimaryEncryptionKeyMetadata.TYPE)) {
            ProjectState projectState = projectResolver.getProjectState(state);
            PrimaryEncryptionKeyMetadata metadata = projectState.metadata().custom(PrimaryEncryptionKeyMetadata.TYPE);

            if (metadata == null && this.cache != KeyCache.EMPTY) {
                logger.debug("primary encryption key cache cleared after snapshot restore");
                this.cache = KeyCache.EMPTY;
            }

            if (metadata != null) {
                Map<String, SecretKey> keysByKeyId = HashMap.newHashMap(metadata.getKeys().size());
                for (String keyId : metadata.getKeys().keySet()) {
                    SecretKey secretKey = metadata.toSecretKey(keyId);
                    assert secretKey != null : "key [" + keyId + "] present in metadata but toSecretKey returned null";
                    keysByKeyId.put(keyId, secretKey);
                }
                this.cache = new KeyCache(metadata.getActiveKeyId(), keysByKeyId);
                logger.debug("primary encryption key cache updated: activeKeyId={}", metadata.getActiveKeyId());
                return;
            }
        }

        if (event.localNodeMaster() && cache.activeKeyId == null) {
            submitInstallKey(state);
        }
    }

    @Override
    @Nullable
    public AesGcmEncryptionService.ActiveKey getActiveKey() {
        KeyCache snapshot = cache;
        if (snapshot.activeKeyId() == null) {
            return null;
        }
        return new AesGcmEncryptionService.ActiveKey(snapshot.activeKeyId(), snapshot.activeKey());
    }

    @Override
    @Nullable
    public SecretKey getKey(String keyId) {
        return cache.keysByKeyId().get(keyId);
    }

    /**
     * Registers a {@link KeyRotationHandler}. Throws if a handler with the same {@link KeyRotationHandler#name()} is already registered.
     */
    public void registerKeyRotationHandler(KeyRotationHandler handler) {
        KeyRotationHandler previous = rotationHandlers.putIfAbsent(handler.name(), handler);
        if (previous != null) {
            throw new IllegalStateException("rotation handler with name [" + handler.name() + "] is already registered");
        }
        logger.debug("registered key rotation handler [{}]", handler.name());
    }

    /**
     * Returns the currently registered rotation handlers. The returned collection is a snapshot.
     */
    public Collection<KeyRotationHandler> getRegisteredHandlers() {
        return List.copyOf(rotationHandlers.values());
    }

    /**
     * Returns the names of currently registered rotation handlers as a snapshot set.
     */
    public Set<String> getRegisteredHandlerNames() {
        return Set.copyOf(rotationHandlers.keySet());
    }

    /**
     * Reads the current {@link PrimaryEncryptionKeyMetadata} from cluster state, or {@code null} if no key has been installed yet.
     */
    @Nullable
    public PrimaryEncryptionKeyMetadata getCurrentMetadata(ClusterState state) {
        ProjectState projectState = projectResolver.getProjectState(state);
        return projectState.metadata().custom(PrimaryEncryptionKeyMetadata.TYPE);
    }

    private boolean checkPekFeatureAvailable(ClusterState state) {
        if (featureService.clusterHasFeature(state, PRIMARY_ENCRYPTION_KEY_FEATURE) == false) {
            logger.debug("not all nodes support primary encryption key feature, waiting for rolling upgrade to complete");
            return false;
        }
        return true;
    }

    /**
     * Submits a {@link InstallKeyTask}. Idempotent: a no-op if already installed.
     */
    public void submitInstallKey(ClusterState state) {
        if (checkPekFeatureAvailable(state)) {
            taskQueue.submitTask("install-primary-encryption-key", new InstallKeyTask(), null);
        }
    }

    /**
     * Submits a {@link BeginRotationTask}. Idempotent: a no-op if a rotation is already in progress.
     */
    public void submitBeginRotation(ClusterState state) {
        if (checkPekFeatureAvailable(state)) {
            taskQueue.submitTask("begin-primary-encryption-key-rotation", new BeginRotationTask(), null);
        }
    }

    /**
     * Submits a {@link RetireKeysTask} that drops any non-active key whose deactivation time is strictly less than
     * {@code cutoffDeactivationMillis}.
     */
    public void submitRetireKeys(ClusterState state, long cutoffDeactivationMillis) {
        if (checkPekFeatureAvailable(state)) {
            taskQueue.submitTask("retire-primary-encryption-keys", new RetireKeysTask(cutoffDeactivationMillis), null);
        }
    }

    private record KeyCache(String activeKeyId, Map<String, SecretKey> keysByKeyId) {
        static final KeyCache EMPTY = new KeyCache(null, Map.of());

        KeyCache {
            assert activeKeyId == null || keysByKeyId.containsKey(activeKeyId);
        }

        @Nullable
        SecretKey activeKey() {
            return activeKeyId != null ? keysByKeyId.get(activeKeyId) : null;
        }
    }

    /**
     * Hierarchy of cluster-state tasks that flow through the PEK task queue. {@link KeyRotationExecutor} dispatches on the concrete type.
     */
    sealed interface KeyRotationTask extends ClusterStateTaskListener permits InstallKeyTask, BeginRotationTask, RetireKeysTask {

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
            PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(newEntries, newKeyId);
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
            PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(retained, activeKeyId);
            logger.info("primary encryption key retire: retained active key [{}], retired keys {}", activeKeyId, new TreeSet<>(retiredIds));
            return Tuple.tuple(putMetadata(clusterState, projectState, metadata), null);
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
        }

        @Override
        public String describeTasks(List<KeyRotationTask> tasks) {
            Map<String, Long> byType = tasks.stream().collect(Collectors.groupingBy(KeyRotationTask::description, Collectors.counting()));
            return "primary encryption key tasks " + byType + " [" + tasks.size() + " pending]";
        }
    }
}
