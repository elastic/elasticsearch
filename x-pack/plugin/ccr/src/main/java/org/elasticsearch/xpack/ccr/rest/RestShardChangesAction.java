/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * A REST handler that retrieves shard changes in a specific index, data stream or alias whose name is
 * provided as a parameter. It handles GET requests to the "/{index}/ccr/shard_changes" endpoint retrieving
 * shard-level changes, such as Translog operations, mapping version, settings version, aliases version,
 * the global checkpoint, maximum sequence number and maximum sequence number of updates or deletes.
 * <p>
 * In the case of a data stream, the first backing index is considered the target for retrieving shard changes.
 * In the case of an alias, the first index that the alias points to is considered the target for retrieving
 * shard changes.
 * <p>
 * Note: This handler is only available for snapshot builds.
 */
public class RestShardChangesAction extends BaseRestHandler {

    private static final long DEFAULT_FROM_SEQ_NO = 0L;
    private static final ByteSizeValue DEFAULT_MAX_BATCH_SIZE = ByteSizeValue.of(32, ByteSizeUnit.MB);
    private static final TimeValue DEFAULT_POLL_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);
    private static final int DEFAULT_MAX_OPERATIONS_COUNT = 1024;
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    private static final TimeValue GET_INDEX_UUID_TIMEOUT = new TimeValue(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    private static final TimeValue SHARD_STATS_TIMEOUT = new TimeValue(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    private static final String INDEX_PARAM_NAME = "index";
    private static final String FROM_SEQ_NO_PARAM_NAME = "from_seq_no";
    private static final String MAX_BATCH_SIZE_PARAM_NAME = "max_batch_size";
    private static final String POLL_TIMEOUT_PARAM_NAME = "poll_timeout";
    private static final String MAX_OPERATIONS_COUNT_PARAM_NAME = "max_operations_count";

    @Override
    public String getName() {
        return "ccr_shard_changes_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/ccr/shard_changes"));
    }

    /**
    * Prepares the request for retrieving shard changes.
    *
    * @param restRequest The REST request.
    * @param client The NodeClient for executing the request.
    * @return A RestChannelConsumer for handling the request.
    * @throws IOException If an error occurs while preparing the request.
    */
    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final var indexAbstractionName = restRequest.param(INDEX_PARAM_NAME);
        final var fromSeqNo = restRequest.paramAsLong(FROM_SEQ_NO_PARAM_NAME, DEFAULT_FROM_SEQ_NO);
        final var maxBatchSize = restRequest.paramAsSize(MAX_BATCH_SIZE_PARAM_NAME, DEFAULT_MAX_BATCH_SIZE);
        final var pollTimeout = restRequest.paramAsTime(POLL_TIMEOUT_PARAM_NAME, DEFAULT_POLL_TIMEOUT);
        final var maxOperationsCount = restRequest.paramAsInt(MAX_OPERATIONS_COUNT_PARAM_NAME, DEFAULT_MAX_OPERATIONS_COUNT);

        // NOTE: we first retrieve the concrete index name in case we are dealing with an alias or data stream.
        // Then we use the concrete index name to retrieve the index UUID and shard stats.
        final CompletableFuture<String> indexNameCompletableFuture = asyncGetIndexName(
            client,
            indexAbstractionName,
            client.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME)
        );
        final CompletableFuture<String> indexUUIDCompletableFuture = indexNameCompletableFuture.thenCompose(
            concreteIndexName -> asyncGetIndexUUID(
                client,
                concreteIndexName,
                client.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME),
                RestUtils.getMasterNodeTimeout(restRequest)
            )
        );
        final CompletableFuture<ShardStats> shardStatsCompletableFuture = indexNameCompletableFuture.thenCompose(
            concreteIndexName -> asyncShardStats(client, concreteIndexName, client.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME))
        );

        return channel -> CompletableFuture.allOf(indexUUIDCompletableFuture, shardStatsCompletableFuture).thenRun(() -> {
            try {
                final String concreteIndexName = indexNameCompletableFuture.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                final String indexUUID = indexUUIDCompletableFuture.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                final ShardStats shardStats = shardStatsCompletableFuture.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                final ShardId shardId = shardStats.getShardRouting().shardId();
                final String expectedHistoryUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);

                final ShardChangesAction.Request shardChangesRequest = shardChangesRequest(
                    concreteIndexName,
                    indexUUID,
                    shardId,
                    expectedHistoryUUID,
                    fromSeqNo,
                    maxBatchSize,
                    pollTimeout,
                    maxOperationsCount
                );
                client.execute(ShardChangesAction.INSTANCE, shardChangesRequest, new RestActionListener<>(channel) {
                    @Override
                    protected void processResponse(final ShardChangesAction.Response response) {
                        channel.sendResponse(
                            new RestResponse(
                                RestStatus.OK,
                                shardChangesResponseToXContent(response, indexAbstractionName, concreteIndexName, shardId)
                            )
                        );
                    }
                });

            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Error while retrieving shard changes", e);
            } catch (TimeoutException te) {
                throw new IllegalStateException("Timeout while waiting for shard stats or index UUID", te);
            }
        }).exceptionally(ex -> {
            channel.sendResponse(
                new RestResponse(
                    RestStatus.BAD_REQUEST,
                    "Failed to process shard changes for index [" + indexAbstractionName + "] " + ex.getMessage()
                )
            );
            return null;
        });
    }

    /**
     * Creates a ShardChangesAction.Request object with the provided parameters.
     *
     * @param indexName The name of the index for which to retrieve shard changes.
     * @param indexUUID The UUID of the index.
     * @param shardId The ShardId for which to retrieve shard changes.
     * @param expectedHistoryUUID The expected history UUID of the shard.
     * @param fromSeqNo The sequence number from which to start retrieving shard changes.
     * @param maxBatchSize The maximum size of a batch of operations to retrieve.
     * @param pollTimeout The maximum time to wait for shard changes.
     * @param maxOperationsCount The maximum number of operations to retrieve in a single request.
     * @return A ShardChangesAction.Request object with the provided parameters.
     */
    private static ShardChangesAction.Request shardChangesRequest(
        final String indexName,
        final String indexUUID,
        final ShardId shardId,
        final String expectedHistoryUUID,
        long fromSeqNo,
        final ByteSizeValue maxBatchSize,
        final TimeValue pollTimeout,
        int maxOperationsCount
    ) {
        final ShardChangesAction.Request shardChangesRequest = new ShardChangesAction.Request(
            new ShardId(new Index(indexName, indexUUID), shardId.id()),
            expectedHistoryUUID
        );
        shardChangesRequest.setFromSeqNo(fromSeqNo);
        shardChangesRequest.setMaxBatchSize(maxBatchSize);
        shardChangesRequest.setPollTimeout(pollTimeout);
        shardChangesRequest.setMaxOperationCount(maxOperationsCount);
        return shardChangesRequest;
    }

    /**
     * Converts the response to XContent JSOn format.
     *
     * @param response The ShardChangesAction response.
     * @param indexAbstractionName The name of the index abstraction.
     * @param concreteIndexName The name of the index.
     * @param shardId The ShardId.
     */
    private static XContentBuilder shardChangesResponseToXContent(
        final ShardChangesAction.Response response,
        final String indexAbstractionName,
        final String concreteIndexName,
        final ShardId shardId
    ) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("index_abstraction", indexAbstractionName);
            builder.field("index", concreteIndexName);
            builder.field("shard_id", shardId);
            builder.field("mapping_version", response.getMappingVersion());
            builder.field("settings_version", response.getSettingsVersion());
            builder.field("aliases_version", response.getAliasesVersion());
            builder.field("global_checkpoint", response.getGlobalCheckpoint());
            builder.field("max_seq_no", response.getMaxSeqNo());
            builder.field("max_seq_no_of_updates_or_deletes", response.getMaxSeqNoOfUpdatesOrDeletes());
            builder.field("took_in_millis", response.getTookInMillis());
            if (response.getOperations() != null && response.getOperations().length > 0) {
                operationsToXContent(response, builder);
            }
            builder.endObject();

            return builder;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts the operations from a ShardChangesAction response to XContent JSON format.
     *
     * @param response The ShardChangesAction response containing the operations to be converted.
     * @param builder The XContentBuilder to which the converted operations will be added.
     * @throws IOException If an error occurs while writing to the XContentBuilder.
     */
    private static void operationsToXContent(final ShardChangesAction.Response response, final XContentBuilder builder) throws IOException {
        builder.field("number_of_operations", response.getOperations().length);
        builder.field("operations");
        builder.startArray();
        for (final Translog.Operation operation : response.getOperations()) {
            builder.startObject();
            builder.field("op_type", operation.opType());
            builder.field("seq_no", operation.seqNo());
            builder.field("primary_term", operation.primaryTerm());
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Execute an asynchronous task using a task supplier and an executor service.
     *
     * @param <T> The type of data to be retrieved.
     * @param task The supplier task that provides the data.
     * @param executorService The executorService service for executing the asynchronous task.
     * @param errorMessage The error message to be thrown if the task execution fails.
     * @return A CompletableFuture that completes with the retrieved data.
     */
    private static <T> CompletableFuture<T> supplyAsyncTask(
        final Supplier<T> task,
        final ExecutorService executorService,
        final String errorMessage
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return task.get();
            } catch (Exception e) {
                throw new ElasticsearchException(errorMessage, e);
            }
        }, executorService);
    }

    /**
     * Asynchronously retrieves the index name for a given index, alias or data stream.
     * If the name represents a data stream, the name of the first backing index is returned.
     * If the name represents an alias, the name of the first index that the alias points to is returned.
     *
     * @param client The NodeClient for executing the asynchronous request.
     * @param indexAbstractionName The name of the index, alias or data stream.
     * @return A CompletableFuture that completes with the retrieved index name.
     */
    private static CompletableFuture<String> asyncGetIndexName(
        final NodeClient client,
        final String indexAbstractionName,
        final ExecutorService executorService
    ) {
        return supplyAsyncTask(() -> {
            final ClusterState clusterState = client.admin()
                .cluster()
                .prepareState(new TimeValue(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .get(GET_INDEX_UUID_TIMEOUT)
                .getState();
            final IndexAbstraction indexAbstraction = clusterState.metadata().getIndicesLookup().get(indexAbstractionName);
            if (indexAbstraction == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid index or data stream name [%s]", indexAbstractionName)
                );
            }
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM
                || indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
                return indexAbstraction.getIndices().getFirst().getName();
            }
            return indexAbstractionName;
        }, executorService, "Error while retrieving index name for index or data stream [" + indexAbstractionName + "]");
    }

    /**
     * Asynchronously retrieves the shard stats for a given index using an executor service.
     *
     * @param client The NodeClient for executing the asynchronous request.
     * @param concreteIndexName The name of the index for which to retrieve shard statistics.
     * @param executorService The executorService service for executing the asynchronous task.
     * @return A CompletableFuture that completes with the retrieved ShardStats.
     * @throws ElasticsearchException If an error occurs while retrieving shard statistics.
     */
    private static CompletableFuture<ShardStats> asyncShardStats(
        final NodeClient client,
        final String concreteIndexName,
        final ExecutorService executorService
    ) {
        return supplyAsyncTask(
            () -> Arrays.stream(client.admin().indices().prepareStats(concreteIndexName).clear().get(SHARD_STATS_TIMEOUT).getShards())
                .max(Comparator.comparingLong(shardStats -> shardStats.getCommitStats().getGeneration()))
                .orElseThrow(() -> new ElasticsearchException("Unable to retrieve shard stats for index: " + concreteIndexName)),
            executorService,
            "Error while retrieving shard stats for index [" + concreteIndexName + "]"
        );
    }

    /**
     * Asynchronously retrieves the index UUID for a given index using an executor service.
     *
     * @param client The NodeClient for executing the asynchronous request.
     * @param concreteIndexName The name of the index for which to retrieve the index UUID.
     * @param executorService The executorService service for executing the asynchronous task.
     * @param masterTimeout The timeout for waiting until the cluster is unblocked.
     * @return A CompletableFuture that completes with the retrieved index UUID.
     * @throws ElasticsearchException If an error occurs while retrieving the index UUID.
     */
    private static CompletableFuture<String> asyncGetIndexUUID(
        final NodeClient client,
        final String concreteIndexName,
        final ExecutorService executorService,
        TimeValue masterTimeout
    ) {
        return supplyAsyncTask(
            () -> client.admin()
                .indices()
                .prepareGetIndex(masterTimeout)
                .setIndices(concreteIndexName)
                .get(GET_INDEX_UUID_TIMEOUT)
                .getSetting(concreteIndexName, IndexMetadata.SETTING_INDEX_UUID),
            executorService,
            "Error while retrieving index UUID for index [" + concreteIndexName + "]"
        );
    }
}
