/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingSlowLog;
import org.elasticsearch.index.SearchSlowLog;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

public class FollowIndexAction extends Action<FollowIndexAction.Request, FollowIndexAction.Response> {

    public static final FollowIndexAction INSTANCE = new FollowIndexAction();
    public static final String NAME = "cluster:admin/xpack/ccr/follow_index";

    private FollowIndexAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest {

        private String leaderIndex;
        private String followIndex;
        private long batchSize = ShardFollowTasksExecutor.DEFAULT_BATCH_SIZE;
        private int concurrentProcessors = ShardFollowTasksExecutor.DEFAULT_CONCURRENT_PROCESSORS;
        private long processorMaxTranslogBytes = ShardFollowTasksExecutor.DEFAULT_MAX_TRANSLOG_BYTES;

        public String getLeaderIndex() {
            return leaderIndex;
        }

        public void setLeaderIndex(String leaderIndex) {
            this.leaderIndex = leaderIndex;
        }

        public String getFollowIndex() {
            return followIndex;
        }

        public void setFollowIndex(String followIndex) {
            this.followIndex = followIndex;
        }

        public long getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(long batchSize) {
            if (batchSize < 1) {
                throw new IllegalArgumentException("Illegal batch_size [" + batchSize + "]");
            }

            this.batchSize = batchSize;
        }

        public void setConcurrentProcessors(int concurrentProcessors) {
            if (concurrentProcessors < 1) {
                throw new IllegalArgumentException("concurrent_processors must be larger than 0");
            }
            this.concurrentProcessors = concurrentProcessors;
        }

        public void setProcessorMaxTranslogBytes(long processorMaxTranslogBytes) {
            if (processorMaxTranslogBytes <= 0) {
                throw new IllegalArgumentException("processor_max_translog_bytes must be larger than 0");
            }
            this.processorMaxTranslogBytes = processorMaxTranslogBytes;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            leaderIndex = in.readString();
            followIndex = in.readString();
            batchSize = in.readVLong();
            processorMaxTranslogBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(leaderIndex);
            out.writeString(followIndex);
            out.writeVLong(batchSize);
            out.writeVLong(processorMaxTranslogBytes);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client, Action<Request, Response> action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        Response() {
        }

        Response(boolean acknowledged) {
            super(acknowledged);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final Client client;
        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final PersistentTasksService persistentTasksService;
        private final IndicesService indicesService;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver, Client client, ClusterService clusterService,
                               PersistentTasksService persistentTasksService, IndicesService indicesService) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.clusterService = clusterService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.persistentTasksService = persistentTasksService;
            this.indicesService = indicesService;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            ClusterState localClusterState = clusterService.state();
            IndexMetaData followIndexMetadata = localClusterState.getMetaData().index(request.followIndex);

            String[] indices = new String[]{request.getLeaderIndex()};
            Map<String, List<String>> remoteClusterIndices = remoteClusterService.groupClusterIndices(indices, s -> false);
            if (remoteClusterIndices.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                // Following an index in local cluster, so use local cluster state to fetch leader IndexMetaData:
                IndexMetaData leaderIndexMetadata = localClusterState.getMetaData().index(request.leaderIndex);
                try {
                    start(request, null, leaderIndexMetadata, followIndexMetadata, listener);
                } catch (IOException e) {
                    listener.onFailure(e);
                    return;
                }
            } else {
                // Following an index in remote cluster, so use remote client to fetch leader IndexMetaData:
                assert remoteClusterIndices.size() == 1;
                Map.Entry<String, List<String>> entry = remoteClusterIndices.entrySet().iterator().next();
                assert entry.getValue().size() == 1;
                String clusterNameAlias = entry.getKey();
                String leaderIndex = entry.getValue().get(0);

                Client remoteClient = client.getRemoteClusterClient(clusterNameAlias);
                ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
                clusterStateRequest.clear();
                clusterStateRequest.metaData(true);
                clusterStateRequest.indices(leaderIndex);
                remoteClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(r -> {
                    ClusterState remoteClusterState = r.getState();
                    IndexMetaData leaderIndexMetadata = remoteClusterState.getMetaData().index(leaderIndex);
                    start(request, clusterNameAlias, leaderIndexMetadata, followIndexMetadata, listener);
                }, listener::onFailure));
            }
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
        void start(Request request, String clusterNameAlias, IndexMetaData leaderIndexMetadata, IndexMetaData followIndexMetadata,
                   ActionListener<Response> handler) throws IOException {
            MapperService mapperService = followIndexMetadata != null ? indicesService.createIndexMapperService(followIndexMetadata) : null;
            validate(request, leaderIndexMetadata, followIndexMetadata, mapperService);
            final int numShards = followIndexMetadata.getNumberOfShards();
            final AtomicInteger counter = new AtomicInteger(numShards);
            final AtomicReferenceArray<Object> responses = new AtomicReferenceArray<>(followIndexMetadata.getNumberOfShards());
            Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
                .filter(e -> ShardFollowTask.HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));for (int i = 0; i < numShards; i++) {
                final int shardId = i;
                String taskId = followIndexMetadata.getIndexUUID() + "-" + shardId;
                ShardFollowTask shardFollowTask = new ShardFollowTask(clusterNameAlias,
                        new ShardId(followIndexMetadata.getIndex(), shardId),
                        new ShardId(leaderIndexMetadata.getIndex(), shardId),
                        request.batchSize, request.concurrentProcessors, request.processorMaxTranslogBytes, filteredHeaders);
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
                                    handler.onResponse(new Response(true));
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
    }

    private static final Set<Setting<?>> WHITELISTED_SETTINGS;

    static {
        Set<Setting<?>> whiteListedSettings = new HashSet<>();
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

        WHITELISTED_SETTINGS = Collections.unmodifiableSet(whiteListedSettings);
    }

    static void validate(Request request, IndexMetaData leaderIndex, IndexMetaData followIndex, MapperService followerMapperService) {
        if (leaderIndex == null) {
            throw new IllegalArgumentException("leader index [" + request.leaderIndex + "] does not exist");
        }
        if (followIndex == null) {
            throw new IllegalArgumentException("follow index [" + request.followIndex + "] does not exist");
        }
        if (leaderIndex.getSettings().getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false) == false) {
            throw new IllegalArgumentException("leader index [" + request.leaderIndex + "] does not have soft deletes enabled");
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
            for (Setting<?> whitelistedSetting : WHITELISTED_SETTINGS) {
                if (whitelistedSetting.match(key)) {
                    iterator.remove();
                    break;
                }
            }
        }
        return settings.build();
    }

}
