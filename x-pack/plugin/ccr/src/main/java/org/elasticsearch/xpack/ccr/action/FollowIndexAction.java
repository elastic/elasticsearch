/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;

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

public class FollowIndexAction extends Action<AcknowledgedResponse> {

    public static final FollowIndexAction INSTANCE = new FollowIndexAction();
    public static final String NAME = "cluster:admin/xpack/ccr/follow_index";

    private FollowIndexAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");
        private static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(NAME, true,
            (args, followerIndex) -> {
                if (args[1] != null) {
                    followerIndex = (String) args[1];
                }
                return new Request((String) args[0], followerIndex, (Integer) args[2], (Integer) args[3], (Long) args[4],
                    (Integer) args[5], (Integer) args[6], (TimeValue) args[7], (TimeValue) args[8]);
        });

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LEADER_INDEX_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FOLLOWER_INDEX_FIELD);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), ShardFollowTask.MAX_BATCH_OPERATION_COUNT);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), ShardFollowTask.MAX_CONCURRENT_READ_BATCHES);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), ShardFollowTask.MAX_BATCH_SIZE_IN_BYTES);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), ShardFollowTask.MAX_CONCURRENT_WRITE_BATCHES);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), ShardFollowTask.MAX_WRITE_BUFFER_SIZE);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), ShardFollowTask.RETRY_TIMEOUT.getPreferredName()),
                ShardFollowTask.RETRY_TIMEOUT, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), ShardFollowTask.IDLE_SHARD_RETRY_DELAY.getPreferredName()),
                ShardFollowTask.IDLE_SHARD_RETRY_DELAY, ObjectParser.ValueType.STRING);
        }

        public static Request fromXContent(XContentParser parser, String followerIndex) throws IOException {
            Request request = PARSER.parse(parser, followerIndex);
            if (followerIndex != null) {
                if (request.followerIndex == null) {
                    request.followerIndex = followerIndex;
                } else {
                    if (request.followerIndex.equals(followerIndex) == false) {
                        throw new IllegalArgumentException("provided follower_index is not equal");
                    }
                }
            }
            return request;
        }

        private String leaderIndex;
        private String followerIndex;
        private int maxBatchOperationCount;
        private int maxConcurrentReadBatches;
        private long maxOperationSizeInBytes;
        private int maxConcurrentWriteBatches;
        private int maxWriteBufferSize;
        private TimeValue retryTimeout;
        private TimeValue idleShardRetryDelay;

        public Request(
            String leaderIndex,
            String followerIndex,
            Integer maxBatchOperationCount,
            Integer maxConcurrentReadBatches,
            Long maxOperationSizeInBytes,
            Integer maxConcurrentWriteBatches,
            Integer maxWriteBufferSize,
            TimeValue retryTimeout,
            TimeValue idleShardRetryDelay) {

            if (leaderIndex == null) {
                throw new IllegalArgumentException("leader_index is missing");
            }
            if (followerIndex == null) {
                throw new IllegalArgumentException("follower_index is missing");
            }
            if (maxBatchOperationCount == null) {
                maxBatchOperationCount = ShardFollowNodeTask.DEFAULT_MAX_BATCH_OPERATION_COUNT;
            }
            if (maxConcurrentReadBatches == null) {
                maxConcurrentReadBatches = ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_READ_BATCHES;
            }
            if (maxOperationSizeInBytes == null) {
                maxOperationSizeInBytes = ShardFollowNodeTask.DEFAULT_MAX_BATCH_SIZE_IN_BYTES;
            }
            if (maxConcurrentWriteBatches == null) {
                maxConcurrentWriteBatches = ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_WRITE_BATCHES;
            }
            if (maxWriteBufferSize == null) {
                maxWriteBufferSize = ShardFollowNodeTask.DEFAULT_MAX_WRITE_BUFFER_SIZE;
            }
            if (retryTimeout == null) {
                retryTimeout = ShardFollowNodeTask.DEFAULT_RETRY_TIMEOUT;
            }
            if (idleShardRetryDelay == null) {
                idleShardRetryDelay = ShardFollowNodeTask.DEFAULT_IDLE_SHARD_RETRY_DELAY;
            }

            if (maxBatchOperationCount < 1) {
                throw new IllegalArgumentException("maxBatchOperationCount must be larger than 0");
            }
            if (maxConcurrentReadBatches < 1) {
                throw new IllegalArgumentException("concurrent_processors must be larger than 0");
            }
            if (maxOperationSizeInBytes <= 0) {
                throw new IllegalArgumentException("processor_max_translog_bytes must be larger than 0");
            }
            if (maxConcurrentWriteBatches < 1) {
                throw new IllegalArgumentException("maxConcurrentWriteBatches must be larger than 0");
            }
            if (maxWriteBufferSize < 1) {
                throw new IllegalArgumentException("maxWriteBufferSize must be larger than 0");
            }

            this.leaderIndex = leaderIndex;
            this.followerIndex = followerIndex;
            this.maxBatchOperationCount = maxBatchOperationCount;
            this.maxConcurrentReadBatches = maxConcurrentReadBatches;
            this.maxOperationSizeInBytes = maxOperationSizeInBytes;
            this.maxConcurrentWriteBatches = maxConcurrentWriteBatches;
            this.maxWriteBufferSize = maxWriteBufferSize;
            this.retryTimeout = retryTimeout;
            this.idleShardRetryDelay = idleShardRetryDelay;
        }

        Request() {
        }

        public String getLeaderIndex() {
            return leaderIndex;
        }

        public String getFollowerIndex() {
            return followerIndex;
        }

        public int getMaxBatchOperationCount() {
            return maxBatchOperationCount;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            leaderIndex = in.readString();
            followerIndex = in.readString();
            maxBatchOperationCount = in.readVInt();
            maxConcurrentReadBatches = in.readVInt();
            maxOperationSizeInBytes = in.readVLong();
            maxConcurrentWriteBatches = in.readVInt();
            maxWriteBufferSize = in.readVInt();
            retryTimeout = in.readOptionalTimeValue();
            idleShardRetryDelay = in.readOptionalTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(leaderIndex);
            out.writeString(followerIndex);
            out.writeVInt(maxBatchOperationCount);
            out.writeVInt(maxConcurrentReadBatches);
            out.writeVLong(maxOperationSizeInBytes);
            out.writeVInt(maxConcurrentWriteBatches);
            out.writeVInt(maxWriteBufferSize);
            out.writeOptionalTimeValue(retryTimeout);
            out.writeOptionalTimeValue(idleShardRetryDelay);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
                builder.field(FOLLOWER_INDEX_FIELD.getPreferredName(), followerIndex);
                builder.field(ShardFollowTask.MAX_BATCH_OPERATION_COUNT.getPreferredName(), maxBatchOperationCount);
                builder.field(ShardFollowTask.MAX_BATCH_SIZE_IN_BYTES.getPreferredName(), maxOperationSizeInBytes);
                builder.field(ShardFollowTask.MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize);
                builder.field(ShardFollowTask.MAX_CONCURRENT_READ_BATCHES.getPreferredName(), maxConcurrentReadBatches);
                builder.field(ShardFollowTask.MAX_CONCURRENT_WRITE_BATCHES.getPreferredName(), maxConcurrentWriteBatches);
                builder.field(ShardFollowTask.RETRY_TIMEOUT.getPreferredName(), retryTimeout.getStringRep());
                builder.field(ShardFollowTask.IDLE_SHARD_RETRY_DELAY.getPreferredName(), idleShardRetryDelay.getStringRep());
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return maxBatchOperationCount == request.maxBatchOperationCount &&
                maxConcurrentReadBatches == request.maxConcurrentReadBatches &&
                maxOperationSizeInBytes == request.maxOperationSizeInBytes &&
                maxConcurrentWriteBatches == request.maxConcurrentWriteBatches &&
                maxWriteBufferSize == request.maxWriteBufferSize &&
                Objects.equals(retryTimeout, request.retryTimeout) &&
                Objects.equals(idleShardRetryDelay, request.idleShardRetryDelay) &&
                Objects.equals(leaderIndex, request.leaderIndex) &&
                Objects.equals(followerIndex, request.followerIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                leaderIndex,
                followerIndex,
                maxBatchOperationCount,
                maxConcurrentReadBatches,
                maxOperationSizeInBytes,
                maxConcurrentWriteBatches,
                maxWriteBufferSize,
                retryTimeout,
                idleShardRetryDelay
            );
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, AcknowledgedResponse> {

        private final Client client;
        private final ThreadPool threadPool;
        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final PersistentTasksService persistentTasksService;
        private final IndicesService indicesService;
        private final CcrLicenseChecker ccrLicenseChecker;

        @Inject
        public TransportAction(
                final Settings settings,
                final ThreadPool threadPool,
                final TransportService transportService,
                final ActionFilters actionFilters,
                final Client client,
                final ClusterService clusterService,
                final PersistentTasksService persistentTasksService,
                final IndicesService indicesService,
                final CcrLicenseChecker ccrLicenseChecker) {
            super(settings, NAME, transportService, actionFilters, Request::new);
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
                                 final Request request,
                                 final ActionListener<AcknowledgedResponse> listener) {
            if (ccrLicenseChecker.isCcrAllowed()) {
                final String[] indices = new String[]{request.leaderIndex};
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
            } else {
                listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            }
        }

        private void followLocalIndex(final Request request,
                                      final ActionListener<AcknowledgedResponse> listener) {
            final ClusterState state = clusterService.state();
            final IndexMetaData followerIndexMetadata = state.getMetaData().index(request.getFollowerIndex());
            // following an index in local cluster, so use local cluster state to fetch leader index metadata
            final IndexMetaData leaderIndexMetadata = state.getMetaData().index(request.getLeaderIndex());
            try {
                start(request, null, leaderIndexMetadata, followerIndexMetadata, listener);
            } catch (final IOException e) {
                listener.onFailure(e);
            }
        }

        private void followRemoteIndex(
                final Request request,
                final String clusterAlias,
                final String leaderIndex,
                final ActionListener<AcknowledgedResponse> listener) {
            final ClusterState state = clusterService.state();
            final IndexMetaData followerIndexMetadata = state.getMetaData().index(request.getFollowerIndex());
            ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadata(
                    client,
                    clusterAlias,
                    leaderIndex,
                    listener::onFailure,
                    leaderIndexMetadata -> {
                        try {
                            start(request, clusterAlias, leaderIndexMetadata, followerIndexMetadata, listener);
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
            Request request,
            String clusterNameAlias,
            IndexMetaData leaderIndexMetadata,
            IndexMetaData followIndexMetadata,
            ActionListener<AcknowledgedResponse> handler) throws IOException {

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
                        request.maxBatchOperationCount, request.maxConcurrentReadBatches, request.maxOperationSizeInBytes,
                        request.maxConcurrentWriteBatches, request.maxWriteBufferSize, request.retryTimeout,
                        request.idleShardRetryDelay, filteredHeaders);
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

    static void validate(Request request,
                         IndexMetaData leaderIndex,
                         IndexMetaData followIndex, MapperService followerMapperService) {
        if (leaderIndex == null) {
            throw new IllegalArgumentException("leader index [" + request.leaderIndex + "] does not exist");
        }
        if (followIndex == null) {
            throw new IllegalArgumentException("follow index [" + request.followerIndex + "] does not exist");
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
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(followIndex.getSettings()) == false) {
            throw new IllegalArgumentException("the following index [" + request.followerIndex + "] is not ready " +
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
