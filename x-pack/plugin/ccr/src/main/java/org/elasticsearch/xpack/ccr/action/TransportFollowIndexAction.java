/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingSlowLog;
import org.elasticsearch.index.SearchSlowLog;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

public class TransportFollowIndexAction extends HandledTransportAction<FollowIndexAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final RemoteClusterService remoteClusterService;
    private final PersistentTasksService persistentTasksService;
    private final IndicesService indicesService;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportFollowIndexAction(
            final Settings settings,
            final ThreadPool threadPool,
            final TransportService transportService,
            final ActionFilters actionFilters,
            final Client client,
            final ClusterService clusterService,
            final PersistentTasksService persistentTasksService,
            final IndicesService indicesService,
            final CcrLicenseChecker ccrLicenseChecker) {
        super(settings, FollowIndexAction.NAME, transportService, actionFilters, FollowIndexAction.Request::new);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.persistentTasksService = persistentTasksService;
        this.indicesService = indicesService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected void doExecute(final Task task,
                             final FollowIndexAction.Request request,
                             final ActionListener<AcknowledgedResponse> listener) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        final String[] indices = new String[]{request.getLeaderIndex()};
        final Map<String, List<String>> remoteClusterIndices = remoteClusterService.groupClusterIndices(indices, s -> false);
        if (remoteClusterIndices.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
            followLocalIndex(request, listener);
        } else {
            assert remoteClusterIndices.size() == 1;
            final Map.Entry<String, List<String>> entry = remoteClusterIndices.entrySet().iterator().next();
            assert entry.getValue().size() == 1;
            final String clusterAlias = entry.getKey();
            final String leaderIndex = entry.getValue().get(0);
            followRemoteIndex(request, clusterAlias, leaderIndex, listener);
        }
    }

    private void followLocalIndex(final FollowIndexAction.Request request,
                                  final ActionListener<AcknowledgedResponse> listener) {
        final ClusterState state = clusterService.state();
        final IndexMetaData followerIndexMetadata = state.getMetaData().index(request.getFollowerIndex());
        // following an index in local cluster, so use local cluster state to fetch leader index metadata
        final IndexMetaData leaderIndexMetadata = state.getMetaData().index(request.getLeaderIndex());
        if (leaderIndexMetadata == null) {
            throw new IndexNotFoundException(request.getFollowerIndex());
        }
        ccrLicenseChecker.fetchLeaderHistoryUUIDs(client, leaderIndexMetadata, listener::onFailure, historyUUIDs -> {
            try {
                start(request, null, leaderIndexMetadata, followerIndexMetadata, historyUUIDs, listener);
            } catch (final IOException e) {
                listener.onFailure(e);
            }
        });
    }

    private void followRemoteIndex(
            final FollowIndexAction.Request request,
            final String clusterAlias,
            final String leaderIndex,
            final ActionListener<AcknowledgedResponse> listener) {
        final ClusterState state = clusterService.state();
        final IndexMetaData followerIndexMetadata = state.getMetaData().index(request.getFollowerIndex());
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
                client,
                clusterAlias,
                leaderIndex,
                listener::onFailure,
                (leaderHistoryUUID, leaderIndexMetadata) -> {
                    try {
                        start(request, clusterAlias, leaderIndexMetadata, followerIndexMetadata, leaderHistoryUUID, listener);
                    } catch (final IOException e) {
                        listener.onFailure(e);
                    }
                });
    }

    /**
     * Performs validation on the provided leader and follow {@link IndexMetaData} instances and then
     * creates a persistent task for each leader primary shard. This persistent tasks track changes in the leader
     * shard and replicate these changes to a follower shard.
     *
     * Currently the following validation is performed:
     * <ul>
     *     <li>The leader index and follow index need to have the same number of primary shards</li>
     * </ul>
     */
    void start(
            FollowIndexAction.Request request,
            String clusterNameAlias,
            IndexMetaData leaderIndexMetadata,
            IndexMetaData followIndexMetadata,
            String[] leaderIndexHistoryUUIDs,
            ActionListener<AcknowledgedResponse> handler) throws IOException {

        MapperService mapperService = followIndexMetadata != null ? indicesService.createIndexMapperService(followIndexMetadata) : null;
        validate(request, leaderIndexMetadata, followIndexMetadata, leaderIndexHistoryUUIDs, mapperService);
        final int numShards = followIndexMetadata.getNumberOfShards();
        final AtomicInteger counter = new AtomicInteger(numShards);
        final AtomicReferenceArray<Object> responses = new AtomicReferenceArray<>(followIndexMetadata.getNumberOfShards());
        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
                .filter(e -> ShardFollowTask.HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        for (int i = 0; i < numShards; i++) {
            final int shardId = i;
            String taskId = followIndexMetadata.getIndexUUID() + "-" + shardId;
            String[] recordedLeaderShardHistoryUUIDs = extractIndexShardHistoryUUIDs(followIndexMetadata);
            String recordedLeaderShardHistoryUUID = recordedLeaderShardHistoryUUIDs[shardId];

            ShardFollowTask shardFollowTask = new ShardFollowTask(
                    clusterNameAlias,
                    new ShardId(followIndexMetadata.getIndex(), shardId),
                    new ShardId(leaderIndexMetadata.getIndex(), shardId),
                    request.getMaxBatchOperationCount(),
                    request.getMaxConcurrentReadBatches(),
                    request.getMaxOperationSizeInBytes(),
                    request.getMaxConcurrentWriteBatches(),
                    request.getMaxWriteBufferSize(),
                    request.getMaxRetryDelay(),
                    request.getIdleShardRetryDelay(),
                    recordedLeaderShardHistoryUUID,
                    filteredHeaders);
            persistentTasksService.sendStartRequest(taskId, ShardFollowTask.NAME, shardFollowTask,
                    new ActionListener<PersistentTasksCustomMetaData.PersistentTask<ShardFollowTask>>() {
                        @Override
                        public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ShardFollowTask> task) {
                            responses.set(shardId, task);
                            finalizeResponse();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            responses.set(shardId, e);
                            finalizeResponse();
                        }

                        void finalizeResponse() {
                            Exception error = null;
                            if (counter.decrementAndGet() == 0) {
                                for (int j = 0; j < responses.length(); j++) {
                                    Object response = responses.get(j);
                                    if (response instanceof Exception) {
                                        if (error == null) {
                                            error = (Exception) response;
                                        } else {
                                            error.addSuppressed((Throwable) response);
                                        }
                                    }
                                }

                                if (error == null) {
                                    // include task ids?
                                    handler.onResponse(new AcknowledgedResponse(true));
                                } else {
                                    // TODO: cancel all started tasks
                                    handler.onFailure(error);
                                }
                            }
                        }
                    }
            );
        }
    }

    static void validate(
            final FollowIndexAction.Request request,
            final IndexMetaData leaderIndex,
            final IndexMetaData followIndex,
            final String[] leaderIndexHistoryUUID,
            final MapperService followerMapperService) {
        if (leaderIndex == null) {
            throw new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist");
        }
        if (followIndex == null) {
            throw new IllegalArgumentException("follow index [" + request.getFollowerIndex() + "] does not exist");
        }

        String[] recordedHistoryUUIDs = extractIndexShardHistoryUUIDs(followIndex);
        assert recordedHistoryUUIDs.length == leaderIndexHistoryUUID.length;
        for (int i = 0; i < leaderIndexHistoryUUID.length; i++) {
            String recordedLeaderIndexHistoryUUID = recordedHistoryUUIDs[i];
            String actualLeaderIndexHistoryUUID = leaderIndexHistoryUUID[i];
            if (recordedLeaderIndexHistoryUUID.equals(actualLeaderIndexHistoryUUID) == false) {
                throw new IllegalArgumentException("leader shard [" + request.getFollowerIndex() + "][" + i + "] should reference [" +
                    recordedLeaderIndexHistoryUUID + "] as history uuid but instead reference [" + actualLeaderIndexHistoryUUID +
                    "] as history uuid");
            }
        }

        if (leaderIndex.getSettings().getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false) == false) {
            throw new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not have soft deletes enabled");
        }
        if (leaderIndex.getNumberOfShards() != followIndex.getNumberOfShards()) {
            throw new IllegalArgumentException("leader index primary shards [" + leaderIndex.getNumberOfShards() +
                    "] does not match with the number of shards of the follow index [" + followIndex.getNumberOfShards() + "]");
        }
        if (leaderIndex.getRoutingNumShards() != followIndex.getRoutingNumShards()) {
            throw new IllegalArgumentException("leader index number_of_routing_shards [" + leaderIndex.getRoutingNumShards() +
                    "] does not match with the number_of_routing_shards of the follow index [" + followIndex.getRoutingNumShards() + "]");
        }
        if (leaderIndex.getState() != IndexMetaData.State.OPEN || followIndex.getState() != IndexMetaData.State.OPEN) {
            throw new IllegalArgumentException("leader and follow index must be open");
        }
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(followIndex.getSettings()) == false) {
            throw new IllegalArgumentException("the following index [" + request.getFollowerIndex() + "] is not ready " +
                    "to follow; the setting [" + CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey() + "] must be enabled.");
        }
        // Make a copy, remove settings that are allowed to be different and then compare if the settings are equal.
        Settings leaderSettings = filter(leaderIndex.getSettings());
        Settings followerSettings = filter(followIndex.getSettings());
        if (leaderSettings.equals(followerSettings) == false) {
            throw new IllegalArgumentException("the leader and follower index settings must be identical");
        }

        // Validates if the current follower mapping is mergable with the leader mapping.
        // This also validates for example whether specific mapper plugins have been installed
        followerMapperService.merge(leaderIndex, MapperService.MergeReason.MAPPING_RECOVERY);
    }

    private static String[] extractIndexShardHistoryUUIDs(IndexMetaData followIndexMetadata) {
        String historyUUIDs = followIndexMetadata.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY)
            .get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS);
        return historyUUIDs.split(",");
    }

    private static final Set<Setting<?>> WHITE_LISTED_SETTINGS;

    static {
        final Set<Setting<?>> whiteListedSettings = new HashSet<>();
        whiteListedSettings.add(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING);
        whiteListedSettings.add(IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING);

        whiteListedSettings.add(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING);
        whiteListedSettings.add(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING);
        whiteListedSettings.add(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING);
        whiteListedSettings.add(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING);
        whiteListedSettings.add(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING);
        whiteListedSettings.add(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING);

        whiteListedSettings.add(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING);
        whiteListedSettings.add(IndexSettings.MAX_RESULT_WINDOW_SETTING);
        whiteListedSettings.add(IndexSettings.INDEX_WARMER_ENABLED_SETTING);
        whiteListedSettings.add(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING);
        whiteListedSettings.add(IndexSettings.MAX_RESCORE_WINDOW_SETTING);
        whiteListedSettings.add(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING);
        whiteListedSettings.add(IndexSettings.DEFAULT_FIELD_SETTING);
        whiteListedSettings.add(IndexSettings.QUERY_STRING_LENIENT_SETTING);
        whiteListedSettings.add(IndexSettings.QUERY_STRING_ANALYZE_WILDCARD);
        whiteListedSettings.add(IndexSettings.QUERY_STRING_ALLOW_LEADING_WILDCARD);
        whiteListedSettings.add(IndexSettings.ALLOW_UNMAPPED);
        whiteListedSettings.add(IndexSettings.INDEX_SEARCH_IDLE_AFTER);
        whiteListedSettings.add(BitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING);

        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING);
        whiteListedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL);
        whiteListedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING);
        whiteListedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING);
        whiteListedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING);
        whiteListedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING);
        whiteListedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL_SETTING);
        whiteListedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING);
        whiteListedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING);

        whiteListedSettings.add(IndexSettings.INDEX_SOFT_DELETES_SETTING);
        whiteListedSettings.add(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING);

        WHITE_LISTED_SETTINGS = Collections.unmodifiableSet(whiteListedSettings);
    }

    private static Settings filter(Settings originalSettings) {
        Settings.Builder settings = Settings.builder().put(originalSettings);
        // Remove settings that are always going to be different between leader and follow index:
        settings.remove(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey());
        settings.remove(IndexMetaData.SETTING_INDEX_UUID);
        settings.remove(IndexMetaData.SETTING_INDEX_PROVIDED_NAME);
        settings.remove(IndexMetaData.SETTING_CREATION_DATE);

        Iterator<String> iterator = settings.keys().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            for (Setting<?> whitelistedSetting : WHITE_LISTED_SETTINGS) {
                if (whitelistedSetting.match(key)) {
                    iterator.remove();
                    break;
                }
            }
        }
        return settings.build();
    }

}
