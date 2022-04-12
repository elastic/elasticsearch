/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IndicesWriteLoadStore implements Closeable {
    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "indices.write_load.store.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting(
        "indices.write_load.store.max_retries",
        3,
        1,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> FLUSH_INTERVAL_SETTING = Setting.timeSetting(
        "indices.write_load.store.flush_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    public static final String INDICES_WRITE_LOAD_DATA_STREAM = ".indices-write-load-distribution";

    public static final String INDICES_WRITE_LOAD_FEATURE_NAME = "indices_write_load";

    public static final SystemDataStreamDescriptor INDICES_WRITE_LOAD_DATA_STREAM_DESCRIPTOR = loadDataStreamDescriptor();

    private static SystemDataStreamDescriptor loadDataStreamDescriptor() {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, loadComposableIndexTemplate())
        ) {
            ComposableIndexTemplate composableIndexTemplate = ComposableIndexTemplate.parse(parser);
            return new SystemDataStreamDescriptor(
                INDICES_WRITE_LOAD_DATA_STREAM,
                "Stores indices write load distribution over time",
                SystemDataStreamDescriptor.Type.INTERNAL,
                composableIndexTemplate,
                Map.of(),
                Collections.emptyList(),
                ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String loadComposableIndexTemplate() {
        try {
            return Streams.readFully(IndicesWriteLoadStore.class.getResourceAsStream("/indices-write-load-distribution.json"))
                .utf8ToString();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load write load distribution index template", e);
        }
    }

    private final Logger logger = LogManager.getLogger(IndicesWriteLoadStore.class);
    private final BulkProcessor bulkProcessor;
    private volatile boolean enabled;

    public IndicesWriteLoadStore(ThreadPool threadPool, Client client, ClusterSettings clusterSettings, Settings settings) {
        this(threadPool, client, clusterSettings, settings, () -> true);
    }

    // Visible for testing
    IndicesWriteLoadStore(
        ThreadPool threadPool,
        Client client,
        ClusterSettings clusterSettings,
        Settings settings,
        Supplier<Boolean> flushCondition
    ) {
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
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
                    logger.warn("Failed to index some indices write load distributions: [{}]", failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                long items = request.numberOfActions();
                logger.warn(new ParameterizedMessage("Failed to index {} items into indices write load index", items), failure);
            }
        }, threadPool, threadPool, () -> {})
            .setBulkActions(-1)
            .setFlushInterval(FLUSH_INTERVAL_SETTING.get(settings))
            .setConcurrentRequests(1)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), MAX_RETRIES_SETTING.get(settings)))
            .setFlushCondition(flushCondition)
            .build();
        this.enabled = ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    void putAsync(List<ShardWriteLoadDistribution> shardWriteLoadDistributions) {
        if (enabled == false) {
            return;
        }

        for (ShardWriteLoadDistribution shardWriteLoadDistribution : shardWriteLoadDistributions) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                shardWriteLoadDistribution.toXContent(builder, ToXContent.EMPTY_PARAMS);
                final var request = new IndexRequest(INDICES_WRITE_LOAD_DATA_STREAM).source(builder).opType(DocWriteRequest.OpType.CREATE);
                bulkProcessor.add(request);
            } catch (IOException exception) {
                logger.warn(
                    new ParameterizedMessage(
                        "failed to queue indices write load distribution item in index [{}]: [{}]",
                        INDICES_WRITE_LOAD_DATA_STREAM,
                        shardWriteLoadDistribution
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
    public void close() throws IOException {
        try {
            bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("failed to shut down indices write load store bulk processor after 10 seconds", e);
        }
    }
}
