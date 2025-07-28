/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.bulk.WriteAckDelay;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
import org.elasticsearch.action.ingest.SimulatePipelineTransportAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper;
import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.ElectionSchedulerFactory;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.JoinValidationService;
import org.elasticsearch.cluster.coordination.LagDetector;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.coordination.MasterHistory;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceComputer;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceReconciler;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.HandshakingTransportAddressConnector;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.health.HealthPeriodicLogger;
import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.health.node.action.TransportHealthNodeAction;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.ThreadPoolMergeExecutorService;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.ingest.IngestSettings;
import org.elasticsearch.monitor.fs.FsHealthService;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.jvm.JvmGcMonitorService;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShutdownProgressTracker;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ProxyConnectionStrategy;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.SniffConnectionStrategy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * Encapsulates all valid cluster level settings.
 */
public final class ClusterSettings extends AbstractScopedSettings {

    public static ClusterSettings createBuiltInClusterSettings() {
        return createBuiltInClusterSettings(Settings.EMPTY);
    }

    public static ClusterSettings createBuiltInClusterSettings(Settings nodeSettings) {
        return new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    public ClusterSettings(final Settings nodeSettings, final Set<Setting<?>> settingsSet) {
        super(nodeSettings, settingsSet, Property.NodeScope);
        addSettingsUpdater(new LoggingSettingUpdater(nodeSettings));
    }

    private static final class LoggingSettingUpdater implements SettingUpdater<Settings> {
        final Predicate<String> loggerPredicate = Loggers.LOG_LEVEL_SETTING::match;
        private final Settings settings;

        LoggingSettingUpdater(Settings settings) {
            this.settings = settings;
        }

        @Override
        public boolean hasChanged(Settings current, Settings previous) {
            return current.filter(loggerPredicate).equals(previous.filter(loggerPredicate)) == false;
        }

        @Override
        public Settings getValue(Settings current, Settings previous) {
            Settings.Builder builder = Settings.builder();
            builder.put(current.filter(loggerPredicate));
            for (String key : previous.keySet()) {
                if (loggerPredicate.test(key) && builder.keys().contains(key) == false) {
                    if (Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).exists(settings) == false) {
                        builder.putNull(key);
                    } else {
                        builder.put(key, Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).get(settings).toString());
                    }
                }
            }
            return builder.build();
        }

        @Override
        public void apply(Settings value, Settings current, Settings previous) {
            for (String key : value.keySet()) {
                assert loggerPredicate.test(key);
                String component = key.substring("logger.".length());
                if ("level".equals(component)) {
                    continue;
                }
                if ("_root".equals(component)) {
                    final String rootLevel = value.get(key);
                    if (rootLevel == null) {
                        Loggers.setLevel(LogManager.getRootLogger(), Loggers.LOG_DEFAULT_LEVEL_SETTING.get(settings));
                    } else {
                        Loggers.setLevel(LogManager.getRootLogger(), rootLevel);
                    }
                } else {
                    Loggers.setLevel(LogManager.getLogger(component), value.get(key));
                }
            }
        }
    }

    public static final Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Stream.of(
        AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
        AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
        BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.THRESHOLD_SETTING,
        DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN,
        DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN,
        DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING,
        DataStreamAutoShardingService.CLUSTER_AUTO_SHARDING_MAX_WRITE_THREADS,
        DataStreamAutoShardingService.CLUSTER_AUTO_SHARDING_MIN_WRITE_THREADS,
        DesiredBalanceComputer.PROGRESS_LOG_INTERVAL_SETTING,
        DesiredBalanceComputer.MAX_BALANCE_COMPUTATION_TIME_DURING_INDEX_CREATION_SETTING,
        DesiredBalanceReconciler.UNDESIRED_ALLOCATIONS_LOG_INTERVAL_SETTING,
        DesiredBalanceReconciler.UNDESIRED_ALLOCATIONS_LOG_THRESHOLD_SETTING,
        BreakerSettings.CIRCUIT_BREAKER_LIMIT_SETTING,
        BreakerSettings.CIRCUIT_BREAKER_OVERHEAD_SETTING,
        BreakerSettings.CIRCUIT_BREAKER_TYPE,
        ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING,
        ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
        EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING,
        FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING,
        FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING,
        FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
        FsRepository.REPOSITORIES_CHUNK_SIZE_SETTING,
        FsRepository.REPOSITORIES_LOCATION_SETTING,
        IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING,
        IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING,
        IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING,
        IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING,
        IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING,
        MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING,
        MappingUpdatedAction.INDICES_MAX_IN_FLIGHT_UPDATES_SETTING,
        Metadata.SETTING_READ_ONLY_SETTING,
        Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING,
        ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE,
        IncrementalBulkService.INCREMENTAL_BULK,
        RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING,
        RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
        RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING,
        RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING,
        RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS,
        RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE,
        RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_FACTOR_READ_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_FACTOR_WRITE_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_READ_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_WRITE_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_MAX_OVERCOMMIT_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING,
        RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING,
        SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING,
        InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING,
        InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING,
        InternalSnapshotsInfoService.INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING,
        DestructiveOperations.REQUIRES_NAME_SETTING,
        NoMasterBlockService.NO_MASTER_BLOCK_SETTING,
        GatewayService.EXPECTED_DATA_NODES_SETTING,
        GatewayService.RECOVER_AFTER_DATA_NODES_SETTING,
        GatewayService.RECOVER_AFTER_TIME_SETTING,
        PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD,
        PersistedClusterStateService.DOCUMENT_PAGE_SIZE,
        NetworkModule.HTTP_DEFAULT_TYPE_SETTING,
        NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING,
        NetworkModule.HTTP_TYPE_SETTING,
        NetworkModule.TRANSPORT_TYPE_SETTING,
        HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS,
        HttpTransportSettings.SETTING_CORS_ENABLED,
        HttpTransportSettings.SETTING_CORS_MAX_AGE,
        HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN,
        HttpTransportSettings.SETTING_HTTP_HOST,
        HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST,
        HttpTransportSettings.SETTING_HTTP_BIND_HOST,
        HttpTransportSettings.SETTING_HTTP_PORT,
        HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT,
        HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS,
        HttpTransportSettings.SETTING_HTTP_COMPRESSION,
        HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL,
        HttpTransportSettings.SETTING_CORS_ALLOW_METHODS,
        HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS,
        HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED,
        HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH,
        HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE,
        HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE,
        HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_COUNT,
        HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE,
        HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH,
        HttpTransportSettings.SETTING_HTTP_SERVER_SHUTDOWN_GRACE_PERIOD,
        HttpTransportSettings.SETTING_HTTP_SERVER_SHUTDOWN_POLL_PERIOD,
        HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT,
        HttpTransportSettings.SETTING_HTTP_RESET_COOKIES,
        HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY,
        HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE,
        HttpTransportSettings.SETTING_HTTP_TCP_KEEP_IDLE,
        HttpTransportSettings.SETTING_HTTP_TCP_KEEP_INTERVAL,
        HttpTransportSettings.SETTING_HTTP_TCP_KEEP_COUNT,
        HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS,
        HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
        HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
        HttpTransportSettings.SETTING_HTTP_TRACE_LOG_INCLUDE,
        HttpTransportSettings.SETTING_HTTP_TRACE_LOG_EXCLUDE,
        HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED,
        HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE,
        HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT,
        HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING,
        HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        IndexModule.NODE_STORE_ALLOW_MMAP,
        IndexSettings.NODE_DEFAULT_REFRESH_INTERVAL_SETTING,
        ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
        ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING,
        ClusterService.USER_DEFINED_METADATA,
        MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
        MasterService.MASTER_SERVICE_STARVATION_LOGGING_THRESHOLD_SETTING,
        SearchService.DEFAULT_SEARCH_TIMEOUT_SETTING,
        SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS,
        TransportSearchAction.SHARD_COUNT_LIMIT_SETTING,
        TransportSearchAction.DEFAULT_PRE_FILTER_SHARD_SIZE,
        RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE,
        SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER,
        RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING,
        RemoteClusterService.REMOTE_NODE_ATTRIBUTE,
        RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE,
        RemoteClusterService.REMOTE_CLUSTER_COMPRESS,
        RemoteClusterService.REMOTE_CLUSTER_COMPRESSION_SCHEME,
        RemoteConnectionStrategy.REMOTE_CONNECTION_MODE,
        ProxyConnectionStrategy.PROXY_ADDRESS,
        ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS,
        ProxyConnectionStrategy.SERVER_NAME,
        SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY,
        SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS,
        SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS,
        TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING,
        ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING,
        SnapshotShutdownProgressTracker.SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING,
        NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING,
        TransportReplicationAction.REPLICATION_INITIAL_RETRY_BACKOFF_BOUND,
        TransportReplicationAction.REPLICATION_RETRY_TIMEOUT,
        TransportSettings.HOST,
        TransportSettings.PUBLISH_HOST,
        TransportSettings.PUBLISH_HOST_PROFILE,
        TransportSettings.BIND_HOST,
        TransportSettings.BIND_HOST_PROFILE,
        TransportSettings.PORT,
        TransportSettings.PORT_PROFILE,
        TransportSettings.PUBLISH_PORT,
        TransportSettings.PUBLISH_PORT_PROFILE,
        TransportSettings.TRANSPORT_COMPRESS,
        TransportSettings.TRANSPORT_COMPRESSION_SCHEME,
        TransportSettings.PING_SCHEDULE,
        TransportSettings.CONNECT_TIMEOUT,
        TransportSettings.DEFAULT_FEATURES_SETTING,
        TransportSettings.TCP_NO_DELAY,
        TransportSettings.TCP_NO_DELAY_PROFILE,
        TransportSettings.TCP_KEEP_ALIVE,
        TransportSettings.TCP_KEEP_ALIVE_PROFILE,
        TransportSettings.TCP_KEEP_IDLE,
        TransportSettings.TCP_KEEP_IDLE_PROFILE,
        TransportSettings.TCP_KEEP_INTERVAL,
        TransportSettings.TCP_KEEP_INTERVAL_PROFILE,
        TransportSettings.TCP_KEEP_COUNT,
        TransportSettings.TCP_KEEP_COUNT_PROFILE,
        TransportSettings.TCP_REUSE_ADDRESS,
        TransportSettings.TCP_REUSE_ADDRESS_PROFILE,
        TransportSettings.TCP_SEND_BUFFER_SIZE,
        TransportSettings.TCP_SEND_BUFFER_SIZE_PROFILE,
        TransportSettings.TCP_RECEIVE_BUFFER_SIZE,
        TransportSettings.TCP_RECEIVE_BUFFER_SIZE_PROFILE,
        TransportSettings.CONNECTIONS_PER_NODE_RECOVERY,
        TransportSettings.CONNECTIONS_PER_NODE_BULK,
        TransportSettings.CONNECTIONS_PER_NODE_REG,
        TransportSettings.CONNECTIONS_PER_NODE_STATE,
        TransportSettings.CONNECTIONS_PER_NODE_PING,
        TransportSettings.TRACE_LOG_EXCLUDE_SETTING,
        TransportSettings.TRACE_LOG_INCLUDE_SETTING,
        TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING,
        TransportSettings.RST_ON_CLOSE,
        NetworkService.NETWORK_SERVER,
        NetworkService.GLOBAL_NETWORK_HOST_SETTING,
        NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING,
        NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING,
        NetworkService.TCP_NO_DELAY,
        NetworkService.TCP_KEEP_ALIVE,
        NetworkService.TCP_KEEP_IDLE,
        NetworkService.TCP_KEEP_INTERVAL,
        NetworkService.TCP_KEEP_COUNT,
        NetworkService.TCP_REUSE_ADDRESS,
        NetworkService.TCP_SEND_BUFFER_SIZE,
        NetworkService.TCP_RECEIVE_BUFFER_SIZE,
        ThreadWatchdog.NETWORK_THREAD_WATCHDOG_INTERVAL,
        ThreadWatchdog.NETWORK_THREAD_WATCHDOG_QUIET_TIME,
        IndexSettings.QUERY_STRING_ANALYZE_WILDCARD,
        IndexSettings.QUERY_STRING_ALLOW_LEADING_WILDCARD,
        ScriptService.SCRIPT_CACHE_SIZE_SETTING,
        ScriptService.SCRIPT_CACHE_EXPIRE_SETTING,
        ScriptService.SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING,
        ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING,
        ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING,
        ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING,
        ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING,
        ScriptService.SCRIPT_MAX_SIZE_IN_BYTES,
        ScriptService.TYPES_ALLOWED_SETTING,
        ScriptService.CONTEXTS_ALLOWED_SETTING,
        IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING,
        IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY,
        IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_EXPIRE,
        IndicesRequestCache.INDICES_CACHE_QUERY_SIZE,
        IndicesRequestCache.INDICES_CACHE_QUERY_EXPIRE,
        HunspellService.HUNSPELL_LAZY_LOAD,
        HunspellService.HUNSPELL_IGNORE_CASE,
        HunspellService.HUNSPELL_DICTIONARY_OPTIONS,
        IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT,
        Environment.PATH_DATA_SETTING,
        Environment.PATH_HOME_SETTING,
        Environment.PATH_LOGS_SETTING,
        Environment.PATH_REPO_SETTING,
        Environment.PATH_SHARED_DATA_SETTING,
        NodeEnvironment.NODE_ID_SEED_SETTING,
        Node.INITIAL_STATE_TIMEOUT_SETTING,
        ShutdownPrepareService.MAXIMUM_SHUTDOWN_TIMEOUT_SETTING,
        ShutdownPrepareService.MAXIMUM_REINDEXING_TIMEOUT_SETTING,
        DiscoveryModule.DISCOVERY_TYPE_SETTING,
        DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING,
        DiscoveryModule.ELECTION_STRATEGY_SETTING,
        SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING,
        SeedHostsResolver.DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING,
        SeedHostsResolver.DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING,
        SearchService.DEFAULT_KEEPALIVE_SETTING,
        SearchService.KEEPALIVE_INTERVAL_SETTING,
        SearchService.MAX_KEEPALIVE_SETTING,
        SearchService.ALLOW_EXPENSIVE_QUERIES,
        SearchService.CCS_VERSION_CHECK_SETTING,
        SearchService.CCS_COLLECT_TELEMETRY,
        MultiBucketConsumerService.MAX_BUCKET_SETTING,
        SearchService.LOW_LEVEL_CANCELLATION_SETTING,
        SearchService.MAX_OPEN_SCROLL_CONTEXT,
        SearchService.ENABLE_REWRITE_AGGS_TO_FILTER_BY_FILTER,
        SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING,
        Node.WRITE_PORTS_FILE_SETTING,
        Node.NODE_EXTERNAL_ID_SETTING,
        Node.NODE_NAME_SETTING,
        Node.NODE_ATTRIBUTES,
        NodeRoleSettings.NODE_ROLES_SETTING,
        AutoCreateIndex.AUTO_CREATE_INDEX_SETTING,
        BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX,
        ClusterName.CLUSTER_NAME_SETTING,
        ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING,
        EsExecutors.NODE_PROCESSORS_SETTING,
        ThreadContext.DEFAULT_HEADERS_SETTING,
        Loggers.LOG_DEFAULT_LEVEL_SETTING,
        Loggers.LOG_LEVEL_SETTING,
        NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING,
        OsService.REFRESH_INTERVAL_SETTING,
        ProcessService.REFRESH_INTERVAL_SETTING,
        JvmService.REFRESH_INTERVAL_SETTING,
        FsService.REFRESH_INTERVAL_SETTING,
        JvmGcMonitorService.ENABLED_SETTING,
        JvmGcMonitorService.REFRESH_INTERVAL_SETTING,
        JvmGcMonitorService.GC_SETTING,
        JvmGcMonitorService.GC_OVERHEAD_WARN_SETTING,
        JvmGcMonitorService.GC_OVERHEAD_INFO_SETTING,
        JvmGcMonitorService.GC_OVERHEAD_DEBUG_SETTING,
        PageCacheRecycler.LIMIT_HEAP_SETTING,
        PageCacheRecycler.WEIGHT_BYTES_SETTING,
        PageCacheRecycler.WEIGHT_INT_SETTING,
        PageCacheRecycler.WEIGHT_LONG_SETTING,
        PageCacheRecycler.WEIGHT_OBJECTS_SETTING,
        PageCacheRecycler.TYPE_SETTING,
        PluginsService.MANDATORY_SETTING,
        BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING,
        BootstrapSettings.MEMORY_LOCK_SETTING,
        BootstrapSettings.CTRLHANDLER_SETTING,
        KeyStoreWrapper.SEED_SETTING,
        IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING,
        IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING,
        IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING,
        IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING,
        IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING,
        ResourceWatcherService.ENABLED,
        ResourceWatcherService.RELOAD_INTERVAL_HIGH,
        ResourceWatcherService.RELOAD_INTERVAL_MEDIUM,
        ResourceWatcherService.RELOAD_INTERVAL_LOW,
        SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING,
        SearchModule.INDICES_MAX_NESTED_DEPTH_SETTING,
        SearchModule.SCRIPTED_METRICS_AGG_ONLY_ALLOWED_SCRIPTS,
        SearchModule.SCRIPTED_METRICS_AGG_ALLOWED_INLINE_SCRIPTS,
        SearchModule.SCRIPTED_METRICS_AGG_ALLOWED_STORED_SCRIPTS,
        SearchService.SEARCH_WORKER_THREADS_ENABLED,
        SearchService.QUERY_PHASE_PARALLEL_COLLECTION_ENABLED,
        ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING,
        ThreadPool.LATE_TIME_INTERVAL_WARN_THRESHOLD_SETTING,
        ThreadPool.SLOW_SCHEDULER_TASK_WARN_THRESHOLD_SETTING,
        ThreadPool.WRITE_THREAD_POOLS_EWMA_ALPHA_SETTING,
        FastVectorHighlighter.SETTING_TV_HIGHLIGHT_MULTI_VALUE,
        Node.BREAKER_TYPE_KEY,
        OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
        IndexGraveyard.SETTING_MAX_TOMBSTONES,
        PersistentTasksClusterService.CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING,
        EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING,
        PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING,
        PeerFinder.DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING,
        ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING,
        ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING,
        ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING,
        ElectionSchedulerFactory.ELECTION_MAX_TIMEOUT_SETTING,
        ElectionSchedulerFactory.ELECTION_DURATION_SETTING,
        Coordinator.PUBLISH_TIMEOUT_SETTING,
        Coordinator.PUBLISH_INFO_TIMEOUT_SETTING,
        Coordinator.SINGLE_NODE_CLUSTER_SEED_HOSTS_CHECK_INTERVAL_SETTING,
        JoinValidationService.JOIN_VALIDATION_CACHE_TIMEOUT_SETTING,
        FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING,
        FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING,
        FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING,
        LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING,
        LeaderChecker.LEADER_CHECK_INTERVAL_SETTING,
        LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING,
        Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION,
        TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING,
        ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING,
        ClusterBootstrapService.UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING,
        LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING,
        HandshakingTransportAddressConnector.PROBE_CONNECT_TIMEOUT_SETTING,
        HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING,
        SnapshotsService.MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING,
        RestoreService.REFRESH_REPO_UUID_ON_RESTORE_SETTING,
        FsHealthService.ENABLED_SETTING,
        FsHealthService.REFRESH_INTERVAL_SETTING,
        FsHealthService.SLOW_PATH_LOGGING_THRESHOLD_SETTING,
        IndexingPressure.MAX_INDEXING_BYTES,
        IndexingPressure.MAX_COORDINATING_BYTES,
        IndexingPressure.MAX_PRIMARY_BYTES,
        IndexingPressure.MAX_REPLICA_BYTES,
        IndexingPressure.SPLIT_BULK_THRESHOLD,
        IndexingPressure.SPLIT_BULK_HIGH_WATERMARK,
        IndexingPressure.SPLIT_BULK_HIGH_WATERMARK_SIZE,
        IndexingPressure.SPLIT_BULK_LOW_WATERMARK,
        IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE,
        ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN,
        DataTier.ENFORCE_DEFAULT_TIER_PREFERENCE_SETTING,
        CoordinationDiagnosticsService.IDENTITY_CHANGES_THRESHOLD_SETTING,
        CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING,
        CoordinationDiagnosticsService.NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING,
        MasterHistory.MAX_HISTORY_AGE_SETTING,
        ReadinessService.PORT,
        HealthNodeTaskExecutor.ENABLED_SETTING,
        LocalHealthMonitor.POLL_INTERVAL_SETTING,
        TransportHealthNodeAction.HEALTH_NODE_TRANSPORT_ACTION_TIMEOUT,
        SimulatePipelineTransportAction.INGEST_NODE_TRANSPORT_ACTION_TIMEOUT,
        WriteAckDelay.WRITE_ACK_DELAY_INTERVAL,
        WriteAckDelay.WRITE_ACK_DELAY_RANDOMNESS_BOUND,
        RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS,
        RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED,
        RemoteClusterPortSettings.HOST,
        RemoteClusterPortSettings.PUBLISH_HOST,
        RemoteClusterPortSettings.BIND_HOST,
        RemoteClusterPortSettings.PORT,
        RemoteClusterPortSettings.PUBLISH_PORT,
        RemoteClusterPortSettings.TCP_KEEP_ALIVE,
        RemoteClusterPortSettings.TCP_KEEP_IDLE,
        RemoteClusterPortSettings.TCP_KEEP_INTERVAL,
        RemoteClusterPortSettings.TCP_KEEP_COUNT,
        RemoteClusterPortSettings.TCP_NO_DELAY,
        RemoteClusterPortSettings.TCP_REUSE_ADDRESS,
        RemoteClusterPortSettings.TCP_SEND_BUFFER_SIZE,
        RemoteClusterPortSettings.MAX_REQUEST_HEADER_SIZE,
        HealthPeriodicLogger.POLL_INTERVAL_SETTING,
        HealthPeriodicLogger.ENABLED_SETTING,
        HealthPeriodicLogger.OUTPUT_MODE_SETTING,
        DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING,
        IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING,
        IndicesClusterStateService.SHARD_LOCK_RETRY_TIMEOUT_SETTING,
        IngestSettings.GROK_WATCHDOG_INTERVAL,
        IngestSettings.GROK_WATCHDOG_MAX_EXECUTION_TIME,
        TDigestExecutionHint.SETTING,
        MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT_SETTING,
        MergePolicyConfig.DEFAULT_MAX_TIME_BASED_MERGED_SEGMENT_SETTING,
        ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING,
        ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING,
        ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING,
        ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING,
        TransportService.ENABLE_STACK_OVERFLOW_AVOIDANCE,
        DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING,
        DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING,
        ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_BUFFER_TIME,
        DataStream.isFailureStoreFeatureFlagEnabled() ? DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING : null
    ).filter(Objects::nonNull).collect(toSet());
}
