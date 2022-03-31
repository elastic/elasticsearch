/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.tasks.TaskInfo.INCLUDE_CANCELLED_PARAM;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Service that can store task results.
 */
public class TaskResultsService {

    private static final Logger logger = LogManager.getLogger(TaskResultsService.class);

    public static final String TASKS_FEATURE_NAME = "tasks";

    public static final String TASK_INDEX = ".tasks";
    public static final String TASK_RESULT_MAPPING_VERSION_META_FIELD = "version";

    public static final SystemIndexDescriptor TASKS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(TASK_INDEX + "*")
        .setPrimaryIndex(TASK_INDEX)
        .setDescription("Task Result Index")
        .setSettings(getTaskResultIndexSettings())
        .setMappings(getTaskResultIndexMappings())
        .setVersionMetaKey(TASK_RESULT_MAPPING_VERSION_META_FIELD)
        .setOrigin(TASKS_ORIGIN)
        .build();

    /**
     * The backoff policy to use when saving a task result fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     */
    static final BackoffPolicy STORE_BACKOFF_POLICY = BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);

    private final Client client;

    private final ThreadPool threadPool;

    @Inject
    public TaskResultsService(Client client, ThreadPool threadPool) {
        this.client = new OriginSettingClient(client, TASKS_ORIGIN);
        this.threadPool = threadPool;
    }

    public void storeResult(TaskResult taskResult, ActionListener<Void> listener) {
        IndexRequestBuilder index = client.prepareIndex(TASK_INDEX).setId(taskResult.getTask().taskId().toString());
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            taskResult.toXContent(builder, new ToXContent.MapParams(Map.of(INCLUDE_CANCELLED_PARAM, "false")));
            index.setSource(builder);
        } catch (IOException e) {
            throw new ElasticsearchException("Couldn't convert task result to XContent for [{}]", e, taskResult.getTask());
        }
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), index, listener);
    }

    private void doStoreResult(Iterator<TimeValue> backoff, IndexRequestBuilder index, ActionListener<Void> listener) {
        index.execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (false == (e instanceof EsRejectedExecutionException) || false == backoff.hasNext()) {
                    listener.onFailure(e);
                } else {
                    TimeValue wait = backoff.next();
                    logger.warn(() -> new ParameterizedMessage("failed to store task result, retrying in [{}]", wait), e);
                    threadPool.schedule(() -> doStoreResult(backoff, index, listener), wait, ThreadPool.Names.SAME);
                }
            }
        });
    }

    private static Settings getTaskResultIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();
    }

    private static XContentBuilder getTaskResultIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(TASK_RESULT_MAPPING_VERSION_META_FIELD, Version.CURRENT.toString());
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("completed");
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject("task");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("action");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("cancellable");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("id");
                            builder.field("type", "long");
                            builder.endObject();

                            builder.startObject("parent_task_id");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("node");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("running_time_in_nanos");
                            builder.field("type", "long");
                            builder.endObject();

                            builder.startObject("start_time_in_millis");
                            builder.field("type", "long");
                            builder.endObject();

                            builder.startObject("type");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("status");
                            builder.field("type", "object");
                            builder.field("enabled", false);
                            builder.endObject();

                            builder.startObject("description");
                            builder.field("type", "text");
                            builder.endObject();

                            builder.startObject("headers");
                            builder.field("type", "object");
                            builder.field("enabled", false);
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("response");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();

                    builder.startObject("error");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + TASK_INDEX + " index mappings", e);
        }
    }
}
