/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.settings;

import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.*;
import org.elasticsearch.cluster.service.InternalClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;

/**
 * Encapsulates all valid cluster level settings.
 */
public final class ClusterSettings {

    private final Map<String, Setting<?>> groupSettings = new HashMap<>();
    private final Map<String, Setting<?>> keySettings = new HashMap<>();
    private final Settings defaults;

    public ClusterSettings(Set<Setting<?>> settingsSet) {
        Settings.Builder builder = Settings.builder();
        for (Setting<?> entry : settingsSet) {
            if (entry.getScope() != Setting.Scope.Cluster) {
                throw new IllegalArgumentException("Setting must be a cluster setting but was: " + entry.getScope());
            }
            if (entry.isGroupSetting()) {
                groupSettings.put(entry.getKey(), entry);
            } else {
                keySettings.put(entry.getKey(), entry);
            }
            builder.put(entry.getKey(), entry.getDefault(Settings.EMPTY));
        }
        this.defaults = builder.build();
    }

    public ClusterSettings() {
        this(BUILT_IN_CLUSTER_SETTINGS);
    }

    /**
     * Returns the {@link Setting} for the given key or <code>null</code> if the setting can not be found.
     */
    public Setting get(String key) {
        Setting<?> setting = keySettings.get(key);
        if (setting == null) {
            for (Map.Entry<String, Setting<?>> entry : groupSettings.entrySet()) {
                if (entry.getValue().match(key)) {
                    return entry.getValue();
                }
            }
        } else {
            return setting;
        }
        return null;
    }

    /**
     * Returns <code>true</code> if the setting for the given key is dynamically updateable. Otherwise <code>false</code>.
     */
    public boolean hasDynamicSetting(String key) {
        final Setting setting = get(key);
        return setting != null && setting.isDynamic();
    }

    /**
     * Returns <code>true</code> if the settings is a logger setting.
     */
    public boolean isLoggerSetting(String key) {
        return key.startsWith("logger.");
    }

    /**
     * Returns the cluster settings defaults
     */
    public Settings getDefaults() {
        return defaults;
    }

    /**
     * Returns a settings object that contains all clustersettings that are not
     * already set in the given source.
     */
    public Settings diff(Settings source) {
        Settings.Builder builder = Settings.builder();
        for (Setting<?> setting : keySettings.values()) {
            if (setting.exists(source) == false) {
                builder.put(setting.getKey(), setting.getRaw(source));
            }
        }
        return builder.build();
    }

    public static Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
    AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
    BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING,
    BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING,
    BalancedShardsAllocator.THRESHOLD_SETTING,
    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING,
    ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
    EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
    EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING,
    ZenDiscovery.REJOIN_ON_MASTER_GONE_SETTING,
    FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING,
    FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING,
    FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
    IndexStoreConfig.INDICES_STORE_THROTTLE_TYPE_SETTING,
    IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING,
    IndicesTTLService.INDICES_TTL_INTERVAL_SETTING,
    MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING,
    MetaData.SETTING_READ_ONLY_SETTING,
    RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE_SETTING,
    RecoverySettings.INDICES_RECOVERY_TRANSLOG_OPS_SETTING,
    RecoverySettings.INDICES_RECOVERY_TRANSLOG_SIZE_SETTING,
    RecoverySettings.INDICES_RECOVERY_COMPRESS_SETTING,
    RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS_SETTING,
    RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS_SETTING,
    RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
    RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING,
    RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING,
    RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
    RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING,
    RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
    ThreadPool.THREADPOOL_GROUP_SETTING,
    ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING,
    ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
    DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
    DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
    DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
    DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING,
    DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING,
    InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING,
    InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING,
    SnapshotInProgressAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED_SETTING,
    DestructiveOperations.REQUIRES_NAME_SETTING,
    DiscoverySettings.PUBLISH_TIMEOUT_SETTING,
    DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING,
    DiscoverySettings.COMMIT_TIMEOUT_SETTING,
    DiscoverySettings.NO_MASTER_BLOCK_SETTING,
    HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
    HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
    HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
    HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
    HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
    InternalClusterService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
    SearchService.DEFAULT_SEARCH_TIMEOUT_SETTING,
    ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
    TransportService.TRACE_LOG_EXCLUDE_SETTING,
    TransportService.TRACE_LOG_INCLUDE_SETTING,
    TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING,
    ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING)));

}
