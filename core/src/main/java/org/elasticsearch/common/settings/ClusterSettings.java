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
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.InternalClusterService;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.gateway.PrimaryShardAllocator;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.repositories.uri.URLRepository;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates all valid cluster level settings.
 */
public final class ClusterSettings extends AbstractScopedSettings {

    public ClusterSettings(Settings settings, Set<Setting<?>> settingsSet) {
        super(settings, settingsSet, Setting.Scope.CLUSTER);
    }

    @Override
    public synchronized Settings applySettings(Settings newSettings) {
        Settings settings = super.applySettings(newSettings);
        try {
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                if (entry.getKey().startsWith("logger.")) {
                    String component = entry.getKey().substring("logger.".length());
                    if ("_root".equals(component)) {
                        ESLoggerFactory.getRootLogger().setLevel(entry.getValue());
                    } else {
                        ESLoggerFactory.getLogger(component).setLevel(entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("failed to refresh settings for [{}]", e, "logger");
        }
        return settings;
    }

    @Override
    public boolean hasDynamicSetting(String key) {
        return isLoggerSetting(key) || super.hasDynamicSetting(key);
    }

    /**
     * Returns <code>true</code> if the settings is a logger setting.
     */
    public boolean isLoggerSetting(String key) {
        return key.startsWith("logger.");
    }


    public static Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
        TransportClientNodesService.CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL, // TODO these transport client settings are kind of odd here and should only be valid if we are a transport client
        TransportClientNodesService.CLIENT_TRANSPORT_PING_TIMEOUT,
        TransportClientNodesService.CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME,
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
        FsRepository.REPOSITORIES_CHUNK_SIZE_SETTING,
        FsRepository.REPOSITORIES_COMPRESS_SETTING,
        FsRepository.REPOSITORIES_LOCATION_SETTING,
        IndexStoreConfig.INDICES_STORE_THROTTLE_TYPE_SETTING,
        IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING,
        IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING,
        IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING,
        IndicesTTLService.INDICES_TTL_INTERVAL_SETTING,
        MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING,
        MetaData.SETTING_READ_ONLY_SETTING,
        RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING,
        RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
        ThreadPool.THREADPOOL_GROUP_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING,
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
        GatewayService.EXPECTED_DATA_NODES_SETTING,
        GatewayService.EXPECTED_MASTER_NODES_SETTING,
        GatewayService.EXPECTED_NODES_SETTING,
        GatewayService.RECOVER_AFTER_DATA_NODES_SETTING,
        GatewayService.RECOVER_AFTER_MASTER_NODES_SETTING,
        GatewayService.RECOVER_AFTER_NODES_SETTING,
        GatewayService.RECOVER_AFTER_TIME_SETTING,
        NetworkModule.HTTP_ENABLED,
        NettyHttpServerTransport.SETTING_CORS_ALLOW_CREDENTIALS,
        NettyHttpServerTransport.SETTING_CORS_ENABLED,
        NettyHttpServerTransport.SETTING_CORS_MAX_AGE,
        NettyHttpServerTransport.SETTING_HTTP_DETAILED_ERRORS_ENABLED,
        NettyHttpServerTransport.SETTING_PIPELINING,
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
        ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING,
        InternalClusterService.CLUSTER_SERVICE_RECONNECT_INTERVAL_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING,
        Transport.TRANSPORT_PROFILES_SETTING,
        Transport.TRANSPORT_TCP_COMPRESS,
        NetworkService.GLOBAL_NETWORK_HOST_SETTING,
        NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING,
        NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING,
        NetworkService.TcpSettings.TCP_NO_DELAY,
        NetworkService.TcpSettings.TCP_KEEP_ALIVE,
        NetworkService.TcpSettings.TCP_REUSE_ADDRESS,
        NetworkService.TcpSettings.TCP_SEND_BUFFER_SIZE,
        NetworkService.TcpSettings.TCP_RECEIVE_BUFFER_SIZE,
        NetworkService.TcpSettings.TCP_BLOCKING,
        NetworkService.TcpSettings.TCP_BLOCKING_SERVER,
        NetworkService.TcpSettings.TCP_BLOCKING_CLIENT,
        NetworkService.TcpSettings.TCP_CONNECT_TIMEOUT,
        IndexSettings.QUERY_STRING_ANALYZE_WILDCARD,
        IndexSettings.QUERY_STRING_ALLOW_LEADING_WILDCARD,
        PrimaryShardAllocator.NODE_INITIAL_SHARDS_SETTING,
        ScriptService.SCRIPT_CACHE_SIZE_SETTING,
        IndicesFieldDataCache.INDICES_FIELDDATA_CLEAN_INTERVAL_SETTING,
        IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY,
        IndicesRequestCache.INDICES_CACHE_QUERY_SIZE,
        IndicesRequestCache.INDICES_CACHE_QUERY_EXPIRE,
        IndicesRequestCache.INDICES_CACHE_REQUEST_CLEAN_INTERVAL,
        HunspellService.HUNSPELL_LAZY_LOAD,
        HunspellService.HUNSPELL_IGNORE_CASE,
        HunspellService.HUNSPELL_DICTIONARY_OPTIONS,
        IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT,
        Environment.PATH_CONF_SETTING,
        Environment.PATH_DATA_SETTING,
        Environment.PATH_HOME_SETTING,
        Environment.PATH_LOGS_SETTING,
        Environment.PATH_PLUGINS_SETTING,
        Environment.PATH_REPO_SETTING,
        Environment.PATH_SCRIPTS_SETTING,
        Environment.PATH_SHARED_DATA_SETTING,
        Environment.PIDFILE_SETTING,
        DiscoveryService.DISCOVERY_SEED_SETTING,
        DiscoveryService.INITIAL_STATE_TIMEOUT_SETTING,
        DiscoveryModule.DISCOVERY_TYPE_SETTING,
        DiscoveryModule.ZEN_MASTER_SERVICE_TYPE_SETTING,
        FaultDetection.PING_RETRIES_SETTING,
        FaultDetection.PING_TIMEOUT_SETTING,
        FaultDetection.REGISTER_CONNECTION_LISTENER_SETTING,
        FaultDetection.PING_INTERVAL_SETTING,
        FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING,
        ZenDiscovery.PING_TIMEOUT_SETTING,
        ZenDiscovery.JOIN_TIMEOUT_SETTING,
        ZenDiscovery.JOIN_RETRY_ATTEMPTS_SETTING,
        ZenDiscovery.JOIN_RETRY_DELAY_SETTING,
        ZenDiscovery.MAX_PINGS_FROM_ANOTHER_MASTER_SETTING,
        ZenDiscovery.SEND_LEAVE_REQUEST_SETTING,
        ZenDiscovery.MASTER_ELECTION_FILTER_CLIENT_SETTING,
        ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING,
        ZenDiscovery.MASTER_ELECTION_FILTER_DATA_SETTING,
        UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING,
        UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING,
        SearchService.DEFAULT_KEEPALIVE_SETTING,
        SearchService.KEEPALIVE_INTERVAL_SETTING,
        Node.WRITE_PORTS_FIELD_SETTING,
        Node.NODE_CLIENT_SETTING,
        Node.NODE_DATA_SETTING,
        Node.NODE_MASTER_SETTING,
        Node.NODE_LOCAL_SETTING,
        Node.NODE_MODE_SETTING,
        Node.NODE_INGEST_SETTING,
        URLRepository.ALLOWED_URLS_SETTING,
        URLRepository.REPOSITORIES_LIST_DIRECTORIES_SETTING,
        URLRepository.REPOSITORIES_URL_SETTING,
        URLRepository.SUPPORTED_PROTOCOLS_SETTING,
        TransportMasterNodeReadAction.FORCE_LOCAL_SETTING,
        AutoCreateIndex.AUTO_CREATE_INDEX_SETTING,
        BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX,
        ClusterName.CLUSTER_NAME_SETTING,
        Client.CLIENT_TYPE_SETTING_S,
        InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING,
        ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING,
        EsExecutors.PROCESSORS_SETTING)));
}
