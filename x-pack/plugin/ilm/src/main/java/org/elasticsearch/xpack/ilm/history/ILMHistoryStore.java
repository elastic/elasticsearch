/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry.INDEX_TEMPLATE_VERSION;

/**
 * The {@link ILMHistoryStore} handles indexing {@link ILMHistoryItem} documents into the
 * appropriate index. It sets up a {@link BulkProcessor} for indexing in bulk, and handles creation
 * of the index/alias as needed for ILM policies.
 */
public class ILMHistoryStore implements Closeable {
    private static final Logger logger = LogManager.getLogger(ILMHistoryStore.class);

    public static final String ILM_HISTORY_ALIAS = "ilm-history-" + INDEX_TEMPLATE_VERSION;

    private final boolean ilmHistoryEnabled;
    private final BulkProcessor processor;
    private final ThreadPool threadPool;

    public ILMHistoryStore(Settings nodeSettings, Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.ilmHistoryEnabled = LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
        this.threadPool = threadPool;

        this.processor = BulkProcessor.builder(
            new OriginSettingClient(client, INDEX_LIFECYCLE_ORIGIN)::bulk,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    // Prior to actually performing the bulk, we should ensure the index exists, and
                    // if we were unable to create it or it was in a bad state, we should not
                    // attempt to index documents.
                    try {
                        final CompletableFuture<Boolean> indexCreated = new CompletableFuture<>();
                        ensureHistoryDataStream(client, clusterService.state(), ActionListener.wrap(indexCreated::complete,
                            ex -> {
                                logger.warn("failed to create ILM history store index prior to issuing bulk request", ex);
                                indexCreated.completeExceptionally(ex);
                            }));
                        indexCreated.get(2, TimeUnit.MINUTES);
                    } catch (Exception e) {
                        logger.warn(new ParameterizedMessage("unable to index the following ILM history items:\n{}",
                            request.requests().stream()
                                .filter(dwr -> (dwr instanceof IndexRequest))
                                .map(dwr -> ((IndexRequest) dwr))
                                .map(IndexRequest::sourceAsMap)
                                .map(Object::toString)
                                .collect(Collectors.joining("\n"))), e);
                        throw new ElasticsearchException(e);
                    }
                    if (logger.isTraceEnabled()) {
                        logger.info("about to index: {}",
                            request.requests().stream()
                                .map(dwr -> ((IndexRequest) dwr).sourceAsMap())
                                .map(Objects::toString)
                                .collect(Collectors.joining(",")));
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    long items = request.numberOfActions();
                    if (logger.isTraceEnabled()) {
                        logger.trace("indexed [{}] items into ILM history index [{}], items: {}", items,
                            Arrays.stream(response.getItems())
                                .map(BulkItemResponse::getIndex)
                                .distinct()
                                .collect(Collectors.joining(",")),
                            request.requests().stream()
                                .map(dwr -> ((IndexRequest) dwr).sourceAsMap())
                                .map(Objects::toString)
                                .collect(Collectors.joining(",")));
                    }
                    if (response.hasFailures()) {
                        IllegalStateException e = new IllegalStateException("failures during bulk request");
                        Arrays.stream(response.getItems())
                            .filter(BulkItemResponse::isFailed)
                            .map(r->r.getFailure().getCause())
                            .forEach(e::addSuppressed);

                        logger.error("failures:", e);
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    long items = request.numberOfActions();
                    logger.error(new ParameterizedMessage("failed to index {} items into ILM history index", items), failure);
                }
            })
            .setBulkActions(100)
            .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(1)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), 3))
            .build();
    }

    /**
     * Attempts to asynchronously index an ILM history entry
     */
    public void putAsync(ILMHistoryItem item) {
        if (ilmHistoryEnabled == false) {
            logger.trace("not recording ILM history item because [{}] is [false]: [{}]",
                LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), item);
            return;
        }
        logger.trace("queueing ILM history item for indexing [{}]: [{}]", ILM_HISTORY_ALIAS, item);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            item.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest request = new IndexRequest(ILM_HISTORY_ALIAS).source(builder).opType(OpType.CREATE);
            // TODO: remove the threadpool wrapping when the .add call is non-blocking
            //  (it can currently execute the bulk request occasionally)
            //  see: https://github.com/elastic/elasticsearch/issues/50440
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    processor.add(request);
                } catch (Exception e) {
                    logger.error(new ParameterizedMessage("failed add ILM history item to queue for index [{}]: [{}]",
                        ILM_HISTORY_ALIAS, item), e);
                }
            });
        } catch (IOException exception) {
            logger.error(new ParameterizedMessage("failed to queue ILM history item in index [{}]: [{}]",
                ILM_HISTORY_ALIAS, item), exception);
        }
    }

    /**
     * Checks if the ILM history data stream exists, and if not, creates it.
     *
     * @param client  The client to use to create the data stream if needed
     * @param state   The current cluster state, to determine if the alias exists
     * @param listener Called after the data stream has been created. `onResponse` called with `true` if the data stream was created,
     *                `false` if it already existed.
     */
    static void ensureHistoryDataStream(Client client, ClusterState state, ActionListener<Boolean> listener) {
        if (state.getMetadata().dataStreams().containsKey(ILM_HISTORY_ALIAS)) {
            listener.onResponse(false);
        } else {
            logger.debug("creating ILM history data stream [{}]", ILM_HISTORY_ALIAS);
            client.execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(ILM_HISTORY_ALIAS),
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        listener.onResponse(true);

                    }

                    @Override
                    public void onFailure(Exception e) {
                        if(e instanceof IllegalArgumentException && e.getMessage().contains("exists")){
                            listener.onResponse(false);
                            logger.debug(
                                "data stream [{}] was created after checking for its existence, likely due to a concurrent call",
                                ILM_HISTORY_ALIAS);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
        }
    }

    @Override
    public void close() {
        try {
            processor.awaitClose(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("failed to shut down ILM history bulk processor after 10 seconds", e);
        }
    }
}
