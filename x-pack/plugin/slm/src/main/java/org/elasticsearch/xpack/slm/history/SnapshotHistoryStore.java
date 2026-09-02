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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry.SLM_TEMPLATE_NAME;

/**
 * Records Snapshot Lifecycle Management actions as represented by {@link SnapshotHistoryItem} into an index
 * for the purposes of querying and alerting.
 */
public class SnapshotHistoryStore implements Closeable {
    private static final Logger logger = LogManager.getLogger(SnapshotHistoryStore.class);

    public static final Setting<ByteSizeValue> MAX_BULK_REQUEST_BYTE_IN_FLIGHT_SETTING = Setting.byteSizeSetting(
        "es.indices.slm.history.bulk.request.bytes.in.flight",
        ByteSizeValue.ofMb(100),
        Setting.Property.NodeScope
    );

    public static final String SLM_HISTORY_DATA_STREAM = ".slm-history-" + INDEX_TEMPLATE_VERSION;

    private static final int SLM_HISTORY_BULK_SIZE = StrictMath.toIntExact(
        ByteSizeValue.parseBytesSizeValue(
            System.getProperty("es.indices.slm.history.bulk.size", "50MB"),
            "es.indices.slm.history.bulk.size"
        ).getBytes()
    );

    private volatile boolean slmHistoryEnabled = true;
    private final ClusterService clusterService;
    private final BulkProcessor2 processor;

    public SnapshotHistoryStore(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this(client, clusterService, threadPool, ActionListener.noop(), TimeValue.timeValueSeconds(5));
    }

    /**
     * For unit testing, allows a more frequent flushInterval
     */
    SnapshotHistoryStore(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionListener<BulkResponse> listener,
        TimeValue flushInterval
    ) {
        this.clusterService = clusterService;
        this.setSlmHistoryEnabled(SLM_HISTORY_INDEX_ENABLED_SETTING.get(clusterService.getSettings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SLM_HISTORY_INDEX_ENABLED_SETTING, this::setSlmHistoryEnabled);

        this.processor = BulkProcessor2.builder(
            new OriginSettingClient(client, INDEX_LIFECYCLE_ORIGIN)::bulk,
            new BulkProcessor2.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    Metadata metadata = clusterService.state().getMetadata();
                    if (metadata.getProject().dataStreams().containsKey(SLM_HISTORY_DATA_STREAM) == false
                        && metadata.getProject().templatesV2().containsKey(SLM_TEMPLATE_NAME) == false) {
                        ElasticsearchException e = new ElasticsearchException(
                            format("data stream [%s] and template [%s] don't exist", SLM_HISTORY_DATA_STREAM, SLM_TEMPLATE_NAME)
                        );
                        logger.warn(
                            () -> format(
                                "unable to index the following SLM history items:\n%s",
                                request.requests()
                                    .stream()
                                    .filter(dwr -> (dwr instanceof IndexRequest))
                                    .map(dwr -> ((IndexRequest) dwr))
                                    .map(IndexRequest::sourceAsMap)
                                    .map(Object::toString)
                                    .collect(joining("\n"))
                            ),
                            e
                        );
                        throw new ElasticsearchException(e);
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "about to index: {}",
                            request.requests()
                                .stream()
                                .map(dwr -> ((IndexRequest) dwr).sourceAsMap())
                                .map(Objects::toString)
                                .collect(Collectors.joining(","))
                        );
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    long items = request.numberOfActions();
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "indexed [{}] items into SLM history index [{}]",
                            items,
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
                        logger.error("failures: [{}]", failures);
                    }
                    listener.onResponse(response);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                    long items = request.numberOfActions();
                    logErrorOrWarning(
                        logger,
                        clusterService.state(),
                        () -> "failed to index " + items + " items into SLM history index",
                        failure
                    );
                    listener.onFailure(failure);
                }
            },
            threadPool
        )
            .setBulkActions(-1)
            .setBulkSize(ByteSizeValue.ofBytes(SLM_HISTORY_BULK_SIZE))
            .setFlushInterval(flushInterval)
            .setMaxBytesInFlight(MAX_BULK_REQUEST_BYTE_IN_FLIGHT_SETTING.get(clusterService.getSettings()))
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
        logger.trace("queueing SLM history item for indexing [{}]: [{}]", SLM_HISTORY_DATA_STREAM, item);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            item.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest request = new IndexRequest(SLM_HISTORY_DATA_STREAM).source(builder).opType(DocWriteRequest.OpType.CREATE);
            processor.add(request);
        } catch (Exception e) {
            logErrorOrWarning(
                logger,
                clusterService.state(),
                () -> format("failed to send SLM history item to index [%s]: [%s]", SLM_HISTORY_DATA_STREAM, item),
                e
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
