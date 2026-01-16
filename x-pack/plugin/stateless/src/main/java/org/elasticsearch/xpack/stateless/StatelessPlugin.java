/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.ClusterModule.DESIRED_BALANCE_ALLOCATOR;
import static org.elasticsearch.cluster.ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING;
import static org.elasticsearch.common.settings.Setting.boolSetting;

public class StatelessPlugin extends Plugin implements ClusterCoordinationPlugin, ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(StatelessPlugin.class);

    public static final LicensedFeature.Persistent STATELESS_FEATURE = LicensedFeature.persistent(
        null,
        "stateless",
        License.OperationMode.ENTERPRISE
    );

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> DATA_STREAMS_LIFECYCLE_ONLY_MODE = boolSetting(
        DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME,
        true,
        Property.NodeScope
    );
    public static final Setting<TimeValue> FAILURE_STORE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        MetadataCreateDataStreamService.FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME,
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    public static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    public static final String NAME = "stateless";

    // Thread pool names are defined in the BlobStoreRepository because we need to verify there that no requests are running on other pools.
    public static final String SHARD_READ_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_READ_THREAD_NAME;
    public static final String SHARD_READ_THREAD_POOL_SETTING = "stateless." + SHARD_READ_THREAD_POOL + "_thread_pool";
    public static final String TRANSLOG_THREAD_POOL = BlobStoreRepository.STATELESS_TRANSLOG_THREAD_NAME;
    public static final String TRANSLOG_THREAD_POOL_SETTING = "stateless." + TRANSLOG_THREAD_POOL + "_thread_pool";
    public static final String SHARD_WRITE_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_WRITE_THREAD_NAME;
    public static final String SHARD_WRITE_THREAD_POOL_SETTING = "stateless." + SHARD_WRITE_THREAD_POOL + "_thread_pool";
    public static final String CLUSTER_STATE_READ_WRITE_THREAD_POOL = BlobStoreRepository.STATELESS_CLUSTER_STATE_READ_WRITE_THREAD_NAME;
    public static final String CLUSTER_STATE_READ_WRITE_THREAD_POOL_SETTING = "stateless."
        + CLUSTER_STATE_READ_WRITE_THREAD_POOL
        + "_thread_pool";
    public static final String GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL = "stateless_get_vbcc_chunk";
    public static final String GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL
        + "_thread_pool";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL = "stateless_fill_vbcc_cache";
    public static final String FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING = "stateless."
        + FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        + "_thread_pool";
    public static final String PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_PREWARMING_THREAD_NAME;
    public static final String PREWARM_THREAD_POOL_SETTING = "stateless." + PREWARM_THREAD_POOL + "_thread_pool";
    public static final String UPLOAD_PREWARM_THREAD_POOL = BlobStoreRepository.STATELESS_SHARD_UPLOAD_PREWARMING_THREAD_NAME;
    public static final String UPLOAD_PREWARM_THREAD_POOL_SETTING = "stateless." + UPLOAD_PREWARM_THREAD_POOL + "_thread_pool";

    /**
     * The set of {@link ShardRouting.Role}s that we expect to see in a stateless deployment
     */
    public static final Set<ShardRouting.Role> STATELESS_SHARD_ROLES = Set.of(ShardRouting.Role.INDEX_ONLY, ShardRouting.Role.SEARCH_ONLY);

    private final boolean enabled;
    private final boolean hasIndexRole;

    public static ExecutorBuilder<?>[] statelessExecutorBuilders(Settings settings, boolean hasIndexRole) {
        // TODO: Consider modifying these pool counts if we change the object store client connections based on node size.
        // Right now we have 10 threads for snapshots, 1 or 8 threads for translog and 20 or 28 threads for shard thread pools. This is to
        // attempt to keep the threads below the default client connections limit of 50. This assumption is currently broken by the snapshot
        // metadata pool having 50 threads. But we will continue to iterate on this numbers and limits.

        final int processors = EsExecutors.allocatedProcessors(settings);
        final int shardReadMaxThreads;
        final int translogCoreThreads;
        final int translogMaxThreads;
        final int shardWriteCoreThreads;
        final int shardWriteMaxThreads;
        final int clusterStateReadWriteCoreThreads;
        final int clusterStateReadWriteMaxThreads;
        final int getVirtualBatchedCompoundCommitChunkCoreThreads;
        final int getVirtualBatchedCompoundCommitChunkMaxThreads;
        final int fillVirtualBatchedCompoundCommitCacheCoreThreads;
        final int fillVirtualBatchedCompoundCommitCacheMaxThreads;
        final int prewarmMaxThreads;
        final int uploadPrewarmCoreThreads;
        final int uploadPrewarmMaxThreads;

        if (hasIndexRole) {
            shardReadMaxThreads = Math.min(processors * 4, 10);
            translogCoreThreads = 2;
            translogMaxThreads = Math.min(processors * 2, 8);
            shardWriteCoreThreads = 2;
            shardWriteMaxThreads = Math.min(processors * 4, 10);
            clusterStateReadWriteCoreThreads = 2;
            clusterStateReadWriteMaxThreads = 4;
            getVirtualBatchedCompoundCommitChunkCoreThreads = 1;
            getVirtualBatchedCompoundCommitChunkMaxThreads = Math.min(processors, 4);
            fillVirtualBatchedCompoundCommitCacheCoreThreads = 0;
            fillVirtualBatchedCompoundCommitCacheMaxThreads = 1;
            prewarmMaxThreads = Math.min(processors * 2, 32);
            // These threads are used for prewarming the shared blob cache on upload, and are separate from the prewarm thread pool
            // in order to avoid any deadlocks between the two (e.g., when two fillgaps compete). Since they are used to prewarm on upload,
            // we use the same amount of max threads as the shard write pool.
            // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
            // threads around to reduce churn and re-use the existing buffers more
            uploadPrewarmMaxThreads = Math.min(processors * 4, 10);
            uploadPrewarmCoreThreads = uploadPrewarmMaxThreads / 2;
        } else {
            shardReadMaxThreads = Math.min(processors * 4, 28);
            translogCoreThreads = 0;
            translogMaxThreads = 1;
            shardWriteCoreThreads = 0;
            shardWriteMaxThreads = 1;
            clusterStateReadWriteCoreThreads = 0;
            clusterStateReadWriteMaxThreads = 1;
            getVirtualBatchedCompoundCommitChunkCoreThreads = 0;
            getVirtualBatchedCompoundCommitChunkMaxThreads = 1;
            prewarmMaxThreads = Math.min(processors * 4, 32);
            // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
            // threads around to reduce churn and re-use the existing buffers more
            fillVirtualBatchedCompoundCommitCacheCoreThreads = Math.max(processors / 2, 2);
            fillVirtualBatchedCompoundCommitCacheMaxThreads = Math.max(processors, 2);
            uploadPrewarmCoreThreads = 0;
            uploadPrewarmMaxThreads = 1;
        }

        return new ExecutorBuilder<?>[] {
            new ScalingExecutorBuilder(
                SHARD_READ_THREAD_POOL,
                4,
                shardReadMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                SHARD_READ_THREAD_POOL_SETTING,
                EsExecutors.TaskTrackingConfig.builder().trackOngoingTasks().trackExecutionTime(0.3).build(),
                EsExecutors.HotThreadsOnLargeQueueConfig.DISABLED
            ),
            new ScalingExecutorBuilder(
                TRANSLOG_THREAD_POOL,
                translogCoreThreads,
                translogMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                TRANSLOG_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                SHARD_WRITE_THREAD_POOL,
                shardWriteCoreThreads,
                shardWriteMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                SHARD_WRITE_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                CLUSTER_STATE_READ_WRITE_THREAD_POOL,
                clusterStateReadWriteCoreThreads,
                clusterStateReadWriteMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                CLUSTER_STATE_READ_WRITE_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL,
                getVirtualBatchedCompoundCommitChunkCoreThreads,
                getVirtualBatchedCompoundCommitChunkMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL,
                fillVirtualBatchedCompoundCommitCacheCoreThreads,
                fillVirtualBatchedCompoundCommitCacheMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                PREWARM_THREAD_POOL,
                // these threads use a sizeable thread-local direct buffer which might take a while to GC, so we prefer to keep some idle
                // threads around to reduce churn and re-use the existing buffers more
                prewarmMaxThreads / 2,
                prewarmMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                PREWARM_THREAD_POOL_SETTING
            ),
            new ScalingExecutorBuilder(
                UPLOAD_PREWARM_THREAD_POOL,
                uploadPrewarmCoreThreads,
                uploadPrewarmMaxThreads,
                TimeValue.timeValueMinutes(5),
                true,
                UPLOAD_PREWARM_THREAD_POOL_SETTING
            ) };
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(STATELESS_ENABLED, DATA_STREAMS_LIFECYCLE_ONLY_MODE, FAILURE_STORE_REFRESH_INTERVAL_SETTING);
    }

    public StatelessPlugin(Settings settings) {
        enabled = STATELESS_ENABLED.get(settings);
        if (enabled) {
            var nonStatelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r) == false)
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (nonStatelessDataNodeRoles.isEmpty() == false) {
                throw new IllegalArgumentException(NAME + " does not support node roles " + nonStatelessDataNodeRoles);
            }
            var statelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(STATELESS_ROLES::contains)
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (statelessDataNodeRoles.size() > 1) {
                throw new IllegalArgumentException(NAME + " does not support a node with more than 1 role of " + statelessDataNodeRoles);
            }
            if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.exists(settings)) {
                if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings)) {
                    throw new IllegalArgumentException(
                        NAME + " does not support " + CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()
                    );
                }
            }
            if (DATA_STREAMS_LIFECYCLE_ONLY_MODE.exists(settings)) {
                if (DATA_STREAMS_LIFECYCLE_ONLY_MODE.get(settings) == false) {
                    throw new IllegalArgumentException(
                        NAME + " does not support setting " + DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey() + " to false"
                    );
                }
            }
            if (Objects.equals(SHARDS_ALLOCATOR_TYPE_SETTING.get(settings), DESIRED_BALANCE_ALLOCATOR) == false) {
                throw new IllegalArgumentException(
                    NAME + " can only be used with " + SHARDS_ALLOCATOR_TYPE_SETTING.getKey() + "=" + DESIRED_BALANCE_ALLOCATOR
                );
            }

            logger.info("[{}] is enabled", NAME);
        } else {
            var statelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r))
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (statelessDataNodeRoles.isEmpty() == false) {
                throw new IllegalArgumentException(
                    NAME + " is not enabled, but stateless-only node roles are configured: " + statelessDataNodeRoles
                );
            }
        }
        hasIndexRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (enabled) {
            final var licenseState = getLicenseState();
            if (STATELESS_FEATURE.checkAndStartTracking(licenseState, NAME) == false) {
                throw new IllegalStateException(
                    NAME
                        + " cannot be enabled with a ["
                        + licenseState.getOperationMode()
                        + "] license. It is only allowed with an Enterprise license."
                );
            }
        }
        return Collections.emptyList();
    }

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Settings additionalSettings() {
        if (enabled) {
            return Settings.builder()
                .put(super.additionalSettings())
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                .put(DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey(), true)
                .put(FAILURE_STORE_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30))
                .build();
        } else {
            return super.additionalSettings();
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (enabled) {
            STATELESS_FEATURE.stopTracking(getLicenseState(), NAME);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (enabled) {
            return List.of(statelessExecutorBuilders(settings, hasIndexRole));
        } else {
            return super.getExecutorBuilders(settings);
        }
    }

}
