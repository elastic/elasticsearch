/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
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
        clusterDynamicSettings.addDynamicSettings(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTES,
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP + "*",
                ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE,
                DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION,
                DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION,
                DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION,
                ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES,
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP + "*",
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP + "*",
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP + "*",
                IndicesFilterCache.INDICES_CACHE_FILTER_SIZE,
                IndicesFilterCache.INDICES_CACHE_FILTER_EXPIRE,
                IndicesStore.INDICES_STORE_THROTTLE_TYPE,
                IndicesStore.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC,
                IndicesTTLService.INDICES_TTL_INTERVAL,
                MetaData.SETTING_READ_ONLY,
                RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE,
                RecoverySettings.INDICES_RECOVERY_TRANSLOG_OPS,
                RecoverySettings.INDICES_RECOVERY_TRANSLOG_SIZE,
                RecoverySettings.INDICES_RECOVERY_COMPRESS,
                RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS,
                RecoverySettings.INDICES_RECOVERY_MAX_SIZE_PER_SEC,
                ThreadPool.THREADPOOL_GROUP + "*",
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES
        );
    }

    public void addDynamicSetting(String... settings) {
        clusterDynamicSettings.addDynamicSettings(settings);
    }

    @Override
    protected void configure() {
        bind(DynamicSettings.class).annotatedWith(ClusterDynamicSettings.class).toInstance(clusterDynamicSettings);
    }
}
