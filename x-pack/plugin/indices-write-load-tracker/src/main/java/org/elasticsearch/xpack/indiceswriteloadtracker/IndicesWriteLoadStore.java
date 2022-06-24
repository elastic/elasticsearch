/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.INDICES_WRITE_LOAD_STORE_ORIGIN;

class IndicesWriteLoadStore implements Closeable {
    static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "indices.write_load.store.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    static final Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting(
        "indices.write_load.store.max_retries",
        3,
        1,
        Setting.Property.NodeScope
    );
    // Average document size is ~450 bytes -> 1Mb / 450b = ~2000 docs
    static final Setting<ByteSizeValue> MAX_BULK_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.write_load.store.max_bulk_size",
        ByteSizeValue.ofMb(1),
        Setting.Property.NodeScope
    );
    static final Setting<Integer> MAX_DOCUMENTS_PER_BULK_SETTING = Setting.intSetting(
        "indices.write_load.store.max_documents",
        20, // TODO: just for testing
        1,
        10000, // That sets an upper bound of ~5Mb
        Setting.Property.NodeScope
    );
    static final Setting<Integer> MAX_CONCURRENT_REQUESTS_SETTING = Setting.intSetting(
        "indices.write_load.store.max_concurrent_requests",
        1,
        0,
        10,
        Setting.Property.NodeScope
    );

    static final String INDICES_WRITE_LOAD_DATA_STREAM = ".indices-write-load";

    static final String INDICES_WRITE_LOAD_FEATURE_NAME = "indices_write_load";

    static final SystemDataStreamDescriptor INDICES_WRITE_LOAD_DATA_STREAM_DESCRIPTOR = loadDataStreamDescriptor();

    private static SystemDataStreamDescriptor loadDataStreamDescriptor() {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, loadComposableIndexTemplate())
        ) {
            ComposableIndexTemplate composableIndexTemplate = ComposableIndexTemplate.parse(parser);
            return new SystemDataStreamDescriptor(
                INDICES_WRITE_LOAD_DATA_STREAM,
                "Stores indices write load over time",
                SystemDataStreamDescriptor.Type.EXTERNAL,
                composableIndexTemplate,
                Map.of(),
                List.of("kibana", "external"),
                ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String loadComposableIndexTemplate() {
        try (var inputStream = IndicesWriteLoadStore.class.getResourceAsStream("/indices-write-load-index-template.json")) {
            return Streams.readFully(inputStream).utf8ToString();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load index write load index template", e);
        }
    }

    private static final Logger logger = LogManager.getLogger(IndicesWriteLoadStore.class);
    private final BulkProcessor bulkProcessor;
    private volatile boolean enabled;

    IndicesWriteLoadStore(ThreadPool threadPool, Client client, ClusterSettings clusterSettings, Settings settings) {
        this(createBulkProcessor(settings, threadPool, client), clusterSettings, settings);
    }

    IndicesWriteLoadStore(BulkProcessor bulkProcessor, ClusterSettings clusterSettings, Settings settings) {
        this.bulkProcessor = bulkProcessor;

        this.enabled = ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    static BulkProcessor createBulkProcessor(Settings settings, ThreadPool threadPool, Client client) {
        return BulkProcessor.builder(new OriginSettingClient(client, INDICES_WRITE_LOAD_STORE_ORIGIN), new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
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
                    logger.warn("Failed to index some indices write load histograms: [{}]", failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                long items = request.numberOfActions();
                logger.warn(String.format(Locale.ROOT, "Failed to index %s items into indices write load index", items), failure);
            }
        }, threadPool, threadPool, () -> {})
            .setBulkActions(MAX_DOCUMENTS_PER_BULK_SETTING.get(settings))
            .setBulkSize(MAX_BULK_SIZE_SETTING.get(settings))
            .setConcurrentRequests(MAX_CONCURRENT_REQUESTS_SETTING.get(settings))
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), MAX_RETRIES_SETTING.get(settings)))
            .build();
    }

    void putAsync(List<ShardWriteLoadHistogramSnapshot> shardWriteLoadHistogramSnapshots) {
        if (enabled == false) {
            return;
        }

        for (ShardWriteLoadHistogramSnapshot shardWriteLoadHistogramSnapshot : shardWriteLoadHistogramSnapshots) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                shardWriteLoadHistogramSnapshot.toXContent(builder, ToXContent.EMPTY_PARAMS);
                final var request = new IndexRequest(INDICES_WRITE_LOAD_DATA_STREAM).source(builder).opType(DocWriteRequest.OpType.CREATE);
                bulkProcessor.add(request);
            } catch (IOException exception) {
                logger.warn(
                    String.format(
                        Locale.ROOT,
                        "failed to queue indices write load distribution item in index [%s]: [%s]",
                        INDICES_WRITE_LOAD_DATA_STREAM,
                        shardWriteLoadHistogramSnapshot
                    ),
                    exception
                );
            }
        }
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public void close() {
        try {
            bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("failed to shut down indices write load store bulk processor after 10 seconds", e);
        }
    }
}
