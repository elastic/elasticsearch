/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.seqno;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * This class implements a REST API for adding a retention lease to a shard in Elasticsearch.
 * Retention leases ensure that specific sequence numbers are retained, even as the global checkpoint
 * advances during indexing. This guarantees seq_no values availability until the retention lease is
 * removed.
 *
 * The API supports adding retention leases to indices, data streams, and index aliases. For data streams
 * or aliases, the first backing index or underlying index is identified, and the retention lease is added
 * to its shard.
 *
 * **Note:** This REST API is available only in Elasticsearch snapshot builds and is intended solely
 * for benchmarking purposes, such as benchmarking operations like the shard changes API in Rally tracks.
 * It is not intended for use in production environments.
 *
 * The response provides details about the added retention lease, including the target index,
 * shard ID, retention lease ID, and source.
 */
public class RestAddRetentionLeaseAction extends BaseRestHandler {

    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    private static final TimeValue SHARD_STATS_TIMEOUT = new TimeValue(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    private static final TimeValue GET_INDEX_TIMEOUT = new TimeValue(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    private static final String INDEX_PARAM = "index";
    private static final String ID_PARAM = "id";
    private static final String SOURCE_PARAM = "source";

    @Override
    public String getName() {
        return "add_retention_lease_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/{index}/seq_no/add_retention_lease"));
    }

    /**
     * Prepare a request to add a retention lease. When the target is an alias or data stream we just
     * get the first shard of the first index using the shard stats api.
     *
     * @param request the request to execute
     * @param client The NodeClient for executing the request.
     * @return A RestChannelConsumer for handling the request.
     * @throws IOException If an error occurs while preparing the request.
     */
    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String indexAbstractionName = request.param(INDEX_PARAM);
        final String retentionLeaseId = request.param(ID_PARAM, UUIDs.randomBase64UUID());
        final String retentionLeaseSource = request.param(SOURCE_PARAM, UUIDs.randomBase64UUID());

        return channel -> asyncGetIndexName(client, indexAbstractionName, client.threadPool().executor(ThreadPool.Names.GENERIC))
            .thenCompose(
                concreteIndexName -> asyncShardStats(client, concreteIndexName, client.threadPool().executor(ThreadPool.Names.GENERIC))
                    .thenCompose(
                        shardStats -> addRetentionLease(
                            channel,
                            client,
                            indexAbstractionName,
                            concreteIndexName,
                            shardStats,
                            retentionLeaseId,
                            retentionLeaseSource
                        )
                    )
            )
            .exceptionally(ex -> {
                final String message = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
                channel.sendResponse(
                    new RestResponse(RestStatus.BAD_REQUEST, "Error adding retention lease for [" + indexAbstractionName + "]: " + message)
                );
                return null;
            });
    }

    private static XContentBuilder addRetentionLeaseResponseToXContent(
        final String indexAbstractionName,
        final String concreteIndexName,
        final ShardId shardId,
        final String retentionLeaseId,
        final String retentionLeaseSource
    ) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("index_abstraction", indexAbstractionName);
            builder.field("index", concreteIndexName);
            builder.field("shard_id", shardId);
            builder.field("id", retentionLeaseId);
            builder.field("source", retentionLeaseSource);
            builder.endObject();

            return builder;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a retention lease to a specific shard in an index, data stream, or alias.
     * This operation is asynchronous and sends the response back to the client through the provided {@link RestChannel}.
     *
     * @param channel The {@link RestChannel} used to send the response back to the client.
     * @param client The {@link NodeClient} used to execute the retention lease addition request.
     * @param indexAbstractionName The name of the index, data stream, or alias for which the retention lease is being added.
     * @param shardStats The {@link ShardStats} of the target shard where the retention lease will be added.
     * @param retentionLeaseId A unique identifier for the retention lease being added. This identifies the lease in future operations.
     * @param retentionLeaseSource A description or source of the retention lease request, often used for auditing or tracing purposes.
     * @return A {@link CompletableFuture} that completes when the operation finishes. If the operation succeeds, the future completes
     *         successfully with {@code null}. If an error occurs, the future completes exceptionally with the corresponding exception.
     * @throws ElasticsearchException If the request fails or encounters an unexpected error.
     */
    private CompletableFuture<Void> addRetentionLease(
        final RestChannel channel,
        final NodeClient client,
        final String indexAbstractionName,
        final String concreteIndexName,
        final ShardStats shardStats,
        final String retentionLeaseId,
        final String retentionLeaseSource
    ) {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        try {
            final ShardId shardId = shardStats.getShardRouting().shardId();
            final RetentionLeaseActions.AddRequest addRetentionLeaseRequest = new RetentionLeaseActions.AddRequest(
                shardId,
                retentionLeaseId,
                RetentionLeaseActions.RETAIN_ALL,
                retentionLeaseSource
            );

            client.execute(RetentionLeaseActions.ADD, addRetentionLeaseRequest, new RestActionListener<>(channel) {

                @Override
                protected void processResponse(final ActionResponse.Empty empty) {
                    completableFuture.complete(null);
                    channel.sendResponse(
                        new RestResponse(
                            RestStatus.OK,
                            addRetentionLeaseResponseToXContent(
                                indexAbstractionName,
                                concreteIndexName,
                                shardId,
                                retentionLeaseId,
                                retentionLeaseSource
                            )
                        )
                    );
                }
            });
        } catch (Exception e) {
            completableFuture.completeExceptionally(
                new ElasticsearchException("Failed to add retention lease for [" + indexAbstractionName + "]", e)
            );
        }
        return completableFuture;
    }

    /**
     * Execute an asynchronous task using a task supplier and an executor service.
     *
     * @param <T> The type of data to be retrieved.
     * @param task The supplier task that provides the data.
     * @param executorService The {@link ExecutorService} for executing the asynchronous task.
     * @param errorMessage The error message to be thrown if the task execution fails.
     * @return A {@link CompletableFuture} that completes with the retrieved data.
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
     * @param client The {@link NodeClient} for executing the asynchronous request.
     * @param indexAbstractionName The name of the index, alias or data stream.
     * @return A {@link CompletableFuture} that completes with the retrieved index name.
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
                .get(GET_INDEX_TIMEOUT)
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
        }, executorService, "Error while retrieving index name for or data stream [" + indexAbstractionName + "]");
    }

    /**
     * Asynchronously retrieves the shard stats for a given index using an executor service.
     *
     * @param client The {@link NodeClient} for executing the asynchronous request.
     * @param concreteIndexName The name of the index for which to retrieve shard statistics.
     * @param executorService The {@link ExecutorService} for executing the asynchronous task.
     * @return A {@link CompletableFuture} that completes with the retrieved ShardStats.
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
                .orElseThrow(() -> new ElasticsearchException("Unable to retrieve shard stats for: " + concreteIndexName)),
            executorService,
            "Error while retrieving shard stats for [" + concreteIndexName + "]"
        );
    }
}
