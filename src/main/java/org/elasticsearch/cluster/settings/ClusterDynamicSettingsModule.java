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

package org.elasticsearch.cluster.settings;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 */
public class ClusterDynamicSettingsModule extends AbstractModule {

    private final DynamicSettings clusterDynamicSettings;

    public ClusterDynamicSettingsModule() {
        clusterDynamicSettings = new DynamicSettings();
        clusterDynamicSettings.addDynamicSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTES);
        clusterDynamicSettings.addDynamicSetting(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP + "*");
        clusterDynamicSettings.addDynamicSetting(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, Validator.FLOAT);
        clusterDynamicSettings.addDynamicSetting(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, Validator.FLOAT);
        clusterDynamicSettings.addDynamicSetting(BalancedShardsAllocator.SETTING_THRESHOLD, Validator.NON_NEGATIVE_FLOAT);
        clusterDynamicSettings.addDynamicSetting(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                ClusterRebalanceAllocationDecider.ALLOCATION_ALLOW_REBALANCE_VALIDATOR);
        clusterDynamicSettings.addDynamicSetting(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, Validator.INTEGER);
        clusterDynamicSettings.addDynamicSetting(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE);
        clusterDynamicSettings.addDynamicSetting(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE);
        clusterDynamicSettings.addDynamicSetting(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION);
        clusterDynamicSettings.addDynamicSetting(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION);
        clusterDynamicSettings.addDynamicSetting(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION);
        clusterDynamicSettings.addDynamicSetting(ZenDiscovery.SETTING_REJOIN_ON_MASTER_GONE, Validator.BOOLEAN);
        clusterDynamicSettings.addDynamicSetting(DiscoverySettings.NO_MASTER_BLOCK);
        clusterDynamicSettings.addDynamicSetting(FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP + "*");
        clusterDynamicSettings.addDynamicSetting(FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP + "*");
        clusterDynamicSettings.addDynamicSetting(FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP + "*");
        clusterDynamicSettings.addDynamicSetting(IndicesFilterCache.INDICES_CACHE_FILTER_SIZE);
        clusterDynamicSettings.addDynamicSetting(IndicesFilterCache.INDICES_CACHE_FILTER_EXPIRE, Validator.TIME);
        clusterDynamicSettings.addDynamicSetting(IndicesFilterCache.INDICES_CACHE_FILTER_CONCURRENCY_LEVEL, Validator.POSITIVE_INTEGER);
        clusterDynamicSettings.addDynamicSetting(IndicesStore.INDICES_STORE_THROTTLE_TYPE);
        clusterDynamicSettings.addDynamicSetting(IndicesStore.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC, Validator.BYTES_SIZE);
        clusterDynamicSettings.addDynamicSetting(IndicesTTLService.INDICES_TTL_INTERVAL, Validator.TIME);
        clusterDynamicSettings.addDynamicSetting(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT, Validator.TIME);
        clusterDynamicSettings.addDynamicSetting(MetaData.SETTING_READ_ONLY);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, Validator.BYTES_SIZE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_TRANSLOG_OPS, Validator.INTEGER);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_TRANSLOG_SIZE, Validator.BYTES_SIZE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_COMPRESS);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS, Validator.POSITIVE_INTEGER);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, Validator.POSITIVE_INTEGER);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC, Validator.BYTES_SIZE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(RecoverySettings.INDICES_RECOVERY_MAX_SIZE_PER_SEC, Validator.BYTES_SIZE);
        clusterDynamicSettings.addDynamicSetting(ThreadPool.THREADPOOL_GROUP + "*");
        clusterDynamicSettings.addDynamicSetting(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES, Validator.INTEGER);
        clusterDynamicSettings.addDynamicSetting(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES, Validator.INTEGER);
        clusterDynamicSettings.addDynamicSetting(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK);
        clusterDynamicSettings.addDynamicSetting(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK);
        clusterDynamicSettings.addDynamicSetting(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, Validator.BOOLEAN);
        clusterDynamicSettings.addDynamicSetting(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS, Validator.BOOLEAN);
        clusterDynamicSettings.addDynamicSetting(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(SnapshotInProgressAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED);
        clusterDynamicSettings.addDynamicSetting(DestructiveOperations.REQUIRES_NAME);
        clusterDynamicSettings.addDynamicSetting(DiscoverySettings.PUBLISH_TIMEOUT, Validator.TIME_NON_NEGATIVE);
        clusterDynamicSettings.addDynamicSetting(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, Validator.MEMORY_SIZE);
        clusterDynamicSettings.addDynamicSetting(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, Validator.MEMORY_SIZE);
        clusterDynamicSettings.addDynamicSetting(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, Validator.NON_NEGATIVE_DOUBLE);
        clusterDynamicSettings.addDynamicSetting(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, Validator.MEMORY_SIZE);
        clusterDynamicSettings.addDynamicSetting(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, Validator.NON_NEGATIVE_DOUBLE);
    }

    public void addDynamicSettings(String... settings) {
        clusterDynamicSettings.addDynamicSettings(settings);
    }

    public void addDynamicSetting(String setting, Validator validator) {
        clusterDynamicSettings.addDynamicSetting(setting, validator);
    }


    @Override
    protected void configure() {
        bind(DynamicSettings.class).annotatedWith(ClusterDynamicSettings.class).toInstance(clusterDynamicSettings);
    }
}
