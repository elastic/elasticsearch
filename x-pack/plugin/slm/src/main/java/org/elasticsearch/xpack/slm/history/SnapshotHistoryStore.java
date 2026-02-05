/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
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

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry.SLM_TEMPLATE_NAME;

/**
 * Records Snapshot Lifecycle Management actions as represented by {@link SnapshotHistoryItem} into an index
 * for the purposes of querying and alerting. It sets up a {@link BulkProcessor2} for indexing in bulk with
 * automatic retries on EsRejectedExecutionException.
 */
public class SnapshotHistoryStore implements Closeable {
    private static final Logger logger = LogManager.getLogger(SnapshotHistoryStore.class);

    public static final Setting<ByteSizeValue> SLM_HISTORY_MAX_BULK_REQUEST_BYTES_IN_FLIGHT_SETTING = Setting.byteSizeSetting(
        "slm.history.bulk.request.bytes.in.flight",
        ByteSizeValue.ofMb(100),
        Setting.Property.NodeScope
    );

    public static final String SLM_HISTORY_DATA_STREAM = ".slm-history-" + INDEX_TEMPLATE_VERSION;

    private static final int SLM_HISTORY_BULK_SIZE = StrictMath.toIntExact(
        ByteSizeValue.parseBytesSizeValue(
            System.getProperty("slm.history.bulk.size", "50MB"),
            "slm.history.bulk.size"
        ).getBytes()
    );

    private final ClusterService clusterService;
    private volatile boolean slmHistoryEnabled = true;
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

        this.processor = BulkProcessor2.builder(client::bulk, new BulkProcessor2.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                final Metadata metadata = clusterService.state().getMetadata();
                if (metadata.getProject().templatesV2().containsKey(SLM_TEMPLATE_NAME) == false) {
                    ElasticsearchException e = new ElasticsearchException("no SLM history template");
                    logger.warn(
                        () -> format(
                            "unable to index the following SLM history items:\n%s",
                            request.requests()
                                .stream()
                                .filter(dwr -> (dwr instanceof IndexRequest))
                                .map(dwr -> ((IndexRequest) dwr))
                                .map(IndexRequest::sourceAsMap)
                                .map(Object::toString)
                                .collect(Collectors.joining("\n"))
                        ),
                        e
                    );
                    throw e;
                }
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "about to index [{}] SLM history items",
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
                        "indexed [{}] items into SLM history data stream [{}]",
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
                    logger.error("failed to index some SLM history items: [{}]", failures);
                }
                listener.onResponse(response);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                long items = request.numberOfActions();
                logErrorOrWarning(
                    logger,
                    clusterService.state(),
                    () -> format("failed to index [%s] items into SLM history data stream [%s]", items, SLM_HISTORY_DATA_STREAM),
                    failure
                );
                listener.onFailure(failure);
            }
        }, threadPool)
            .setBulkActions(-1)
            .setBulkSize(ByteSizeValue.ofBytes(SLM_HISTORY_BULK_SIZE))
            .setFlushInterval(flushInterval)
            .setMaxBytesInFlight(SLM_HISTORY_MAX_BULK_REQUEST_BYTES_IN_FLIGHT_SETTING.get(clusterService.getSettings()))
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
            IndexRequest request = new IndexRequest(SLM_HISTORY_DATA_STREAM).opType(DocWriteRequest.OpType.CREATE).source(builder);
            processor.add(request);
        } catch (Exception e) {
            logger.error(() -> format("failed to queue SLM history item for indexing [%s]: [%s]", SLM_HISTORY_DATA_STREAM, item), e);
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

    // On node shutdown, some operations are expected to fail, we log a warning instead of error during node shutdown for those exceptions
    public static void logErrorOrWarning(Logger logger, ClusterState clusterState, Supplier<?> failureMsgSupplier, Exception exception) {
        if (PluginShutdownService.isLocalNodeShutdown(clusterState)) {
            logger.warn(failureMsgSupplier, exception);
        } else {
            logger.error(failureMsgSupplier, exception);
        }
    }

    public void setSlmHistoryEnabled(boolean slmHistoryEnabled) {
        this.slmHistoryEnabled = slmHistoryEnabled;
    }
}
