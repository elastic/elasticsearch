/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.history;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry.SLM_TEMPLATE_NAME;

/**
 * Records Snapshot Lifecycle Management actions as represented by {@link SnapshotHistoryItem} into an index
 * for the purposes of querying and alerting.
 */
public class SnapshotHistoryStore implements Closeable {
    private static final Logger logger = LogManager.getLogger(SnapshotHistoryStore.class);

    public static final String SLM_HISTORY_DATA_STREAM = ".slm-history-" + INDEX_TEMPLATE_VERSION;

    private final ClusterService clusterService;
    private final BulkProcessor2 processor;
    private volatile boolean slmHistoryEnabled = true;

    public SnapshotHistoryStore(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this(client, clusterService, threadPool, null);
    }

    SnapshotHistoryStore(Client client, ClusterService clusterService, ThreadPool threadPool, BulkProcessor2.Listener listenerOverride) {
        this.clusterService = clusterService;
        this.setSlmHistoryEnabled(SLM_HISTORY_INDEX_ENABLED_SETTING.get(clusterService.getSettings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SLM_HISTORY_INDEX_ENABLED_SETTING, this::setSlmHistoryEnabled);

        BulkProcessor2.Listener listener = listenerOverride != null ? listenerOverride : new BulkProcessor2.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                if (clusterService.state().getMetadata().getProject().templatesV2().containsKey(SLM_TEMPLATE_NAME) == false) {
                    ElasticsearchException e = new ElasticsearchException("no SLM history template");
                    logger.warn(
                        () -> format(
                            "unable to index the following SLM history items:\n%s",
                            request.requests()
                                .stream()
                                .filter(dwr -> dwr instanceof IndexRequest)
                                .map(dwr -> ((IndexRequest) dwr).sourceAsMap())
                                .map(Object::toString)
                                .collect(Collectors.joining("\n"))
                        ),
                        e
                    );
                    throw new ElasticsearchException(e);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "indexed [{}] SLM history items into [{}]",
                        request.numberOfActions(),
                        Arrays.stream(response.getItems()).map(BulkItemResponse::getIndex).distinct().collect(Collectors.joining(","))
                    );
                }
                if (response.hasFailures()) {
                    Map<String, String> failures = Arrays.stream(response.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .collect(
                            Collectors.toMap(
                                BulkItemResponse::getId,
                                BulkItemResponse::getFailureMessage,
                                (msg1, msg2) -> Objects.equals(msg1, msg2) ? msg1 : msg1 + "," + msg2
                            )
                        );
                    logger.error("SLM history indexing failures: [{}]", failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                logger.error(() -> format("failed to index [%d] SLM history items", request.numberOfActions()), failure);
            }
        };

        this.processor = BulkProcessor2.builder(
            new OriginSettingClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN)::bulk,
            listener,
            threadPool
        )
            .setBulkActions(-1)
            .setFlushInterval(org.elasticsearch.core.TimeValue.timeValueSeconds(5))
            .setMaxNumberOfRetries(3)
            .build();
    }

    /**
     * Attempts to asynchronously index a snapshot lifecycle management history entry
     *
     * @param item The entry to index
     */
    public void putAsync(SnapshotHistoryItem item) {
        if (slmHistoryEnabled == false) {
            logger.trace(
                "not recording snapshot history item because [{}] is [false]: [{}]",
                SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(),
                item
            );
            return;
        }
        logger.trace("queueing snapshot history item for indexing in data stream [{}]: [{}]", SLM_HISTORY_DATA_STREAM, item);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            item.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest request = new IndexRequest(SLM_HISTORY_DATA_STREAM).opType(DocWriteRequest.OpType.CREATE).source(builder);
            processor.add(request);
        } catch (IOException exception) {
            logger.error(
                () -> format("failed to queue snapshot history item in data stream [%s]: [%s]", SLM_HISTORY_DATA_STREAM, item),
                exception
            );
        }
    }

    @Override
    public void close() {
        try {
            processor.awaitClose(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("failed to shut down SLM history bulk processor after 10 seconds", e);
            Thread.currentThread().interrupt();
        }
    }

    // On node shutdown, some operations are expected to fail, we log a warning instead of error during node shutdown for those exceptions;
    // also we expect some operations to fail due to backpressure, and these need not alert anyone either
    public static void logErrorOrWarning(Logger logger, ClusterState clusterState, Supplier<?> failureMsgSupplier, Exception exception) {
        final var cause = ExceptionsHelper.unwrapCause(exception);
        final Level level;
        if (cause instanceof CircuitBreakingException || cause instanceof EsRejectedExecutionException) {
            level = Level.WARN;
        } else if (PluginShutdownService.isLocalNodeShutdown(clusterState)) {
            level = Level.WARN;
        } else {
            level = Level.ERROR;
        }

        logger.log(level, failureMsgSupplier, exception);
    }

    public void setSlmHistoryEnabled(boolean slmHistoryEnabled) {
        this.slmHistoryEnabled = slmHistoryEnabled;
    }
}
