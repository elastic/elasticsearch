/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ResumeReindexAction;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class SearchServiceReindexKeepAliveMetricsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @After
    public void resetTelemetry() {
        getTelemetryPlugin().resetMeter();
    }

    /**
     * When the reaper closes an expired reindex-backed scroll context after keep-alive, only the scroll keep-alive counter increments.
     */
    public void testIncrementsScrollMetricWhenReindexScrollContextKeepAliveExpires() throws Exception {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final SearchShardTask shardTask = registerReindexBackedShardTask();
        final ScrollShardSearchRequest request = new ScrollShardSearchRequest(indexShard.shardId());
        final ReaderContext context = searchService.createAndPutReaderContext(
            request,
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            1L,
            shardTask
        );
        try {
            Thread.sleep(50L);
            searchService.runKeepAliveReaperOnceForTesting();
            assertBusy(() -> {
                assertThat(sumLongCounter(SearchService.REINDEX_SCROLL_CONTEXT_CLOSED_KEEPALIVE_TOTAL), equalTo(1L));
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_PIT_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
            });
        } finally {
            unregisterReindexAncestorIfPresent(shardTask);
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * Expired scroll contexts without a reindex ancestor do not increment reindex keep-alive metrics.
     */
    public void testNoMetricsWhenScrollContextExpiresWithoutReindexTask() throws Exception {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final SearchShardTask shardTask = new SearchShardTask(
            randomNonNegativeLong(),
            "test",
            "indices:data/read/search[phase/query]",
            "shard",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        final ScrollShardSearchRequest request = new ScrollShardSearchRequest(indexShard.shardId());
        final ReaderContext context = searchService.createAndPutReaderContext(
            request,
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            1L,
            shardTask
        );
        try {
            Thread.sleep(50L);
            searchService.runKeepAliveReaperOnceForTesting();
            assertBusy(() -> {
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_SCROLL_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_PIT_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
            });
        } finally {
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * When the reaper closes an expired reindex-backed PIT context after keep-alive, only the PIT keep-alive counter increments.
     */
    public void testIncrementsPitMetricWhenReindexPitContextKeepAliveExpires() throws Exception {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final SearchShardTask shardTask = registerReindexBackedShardTask();
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            request,
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            1L,
            shardTask
        );
        try {
            Thread.sleep(50L);
            searchService.runKeepAliveReaperOnceForTesting();
            assertBusy(() -> {
                assertThat(sumLongCounter(SearchService.REINDEX_PIT_CONTEXT_CLOSED_KEEPALIVE_TOTAL), equalTo(1L));
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_SCROLL_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
            });
        } finally {
            unregisterReindexAncestorIfPresent(shardTask);
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * If the registered reindex parent is removed before opening the reader context, keep-alive expiry does not emit reindex metrics.
     */
    public void testNoMetricsWhenReindexParentUnregisteredBeforeOpeningContext() throws Exception {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final TaskManager taskManager = getInstanceFromNode(TransportService.class).getTaskManager();
        final Task reindexTask = taskManager.register("test", ReindexAction.NAME, emptyTaskAwareRequest());
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), reindexTask.getId());
        final SearchShardTask shardTask = new SearchShardTask(
            randomNonNegativeLong(),
            "test",
            "indices:data/read/search[phase/query]",
            "shard",
            parentTaskId,
            Map.of()
        );
        taskManager.unregister(reindexTask);
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            request,
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            1L,
            shardTask
        );
        try {
            assertFalse(context.openedUnderReindexingTask());
            Thread.sleep(50L);
            searchService.runKeepAliveReaperOnceForTesting();
            assertBusy(() -> {
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_SCROLL_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_PIT_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
            });
        } finally {
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * Resume-reindex is treated like reindex for keep-alive reaper metrics on an expired PIT context.
     */
    public void testIncrementsPitMetricWhenResumeReindexContextKeepAliveExpires() throws Exception {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final SearchShardTask shardTask = registerSearchShardTaskWithAncestorAction(ResumeReindexAction.NAME);
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            request,
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            1L,
            shardTask
        );
        try {
            Thread.sleep(50L);
            searchService.runKeepAliveReaperOnceForTesting();
            assertBusy(() -> {
                assertThat(sumLongCounter(SearchService.REINDEX_PIT_CONTEXT_CLOSED_KEEPALIVE_TOTAL), equalTo(1L));
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_SCROLL_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
            });
        } finally {
            unregisterReindexAncestorIfPresent(shardTask);
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * Contexts closed after relocation grace are not attributed to keep-alive expiry, so reindex keep-alive metrics stay zero.
     */
    public void testNoKeepAliveMetricsWhenRelocatingContextClosedAfterGrace() throws Exception {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final SearchShardTask shardTask = registerReindexBackedShardTask();
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            request,
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            1L,
            shardTask
        );
        try {
            context.relocate();
            Thread.sleep(1500L);
            searchService.runKeepAliveReaperOnceForTesting();
            assertBusy(() -> {
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_SCROLL_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
                assertThat(
                    getTelemetryPlugin().getLongCounterMeasurement(SearchService.REINDEX_PIT_CONTEXT_CLOSED_KEEPALIVE_TOTAL),
                    empty()
                );
                assertThat(searchService.getActiveContexts(), equalTo(0));
            });
        } finally {
            unregisterReindexAncestorIfPresent(shardTask);
            searchService.freeReaderContext(context.id());
        }
    }

    private long sumLongCounter(String name) {
        long sum = 0L;
        for (Measurement measurement : getTelemetryPlugin().getLongCounterMeasurement(name)) {
            sum += measurement.getLong();
        }
        return sum;
    }

    private SearchShardTask registerReindexBackedShardTask() {
        return registerSearchShardTaskWithAncestorAction(ReindexAction.NAME);
    }

    private SearchShardTask registerSearchShardTaskWithAncestorAction(String ancestorAction) {
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final TransportService transportService = getInstanceFromNode(TransportService.class);
        final TaskManager taskManager = transportService.getTaskManager();
        final Task ancestorTask = taskManager.register("test", ancestorAction, emptyTaskAwareRequest());
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), ancestorTask.getId());
        return new SearchShardTask(
            randomNonNegativeLong(),
            "test",
            "indices:data/read/search[phase/query]",
            "shard",
            parentTaskId,
            Map.of()
        );
    }

    private static TaskAwareRequest emptyTaskAwareRequest() {
        return new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }
        };
    }

    private void unregisterReindexAncestorIfPresent(SearchShardTask shardTask) {
        final TaskManager taskManager = getInstanceFromNode(TransportService.class).getTaskManager();
        TaskId current = shardTask.getParentTaskId();
        while (current.isSet()) {
            final Task parent = taskManager.getTask(current.getId());
            if (parent == null) {
                break;
            }
            current = parent.getParentTaskId();
            taskManager.unregister(parent);
        }
    }

    private TestTelemetryPlugin getTelemetryPlugin() {
        final List<TestTelemetryPlugin> plugins = getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class)
            .toList();
        assertThat(plugins.size(), equalTo(1));
        return plugins.get(0);
    }

    private static final class ScrollShardSearchRequest extends ShardSearchRequest {
        ScrollShardSearchRequest(ShardId shardId) {
            super(
                OriginalIndices.NONE,
                new SearchRequest().allowPartialSearchResults(true),
                shardId,
                0,
                1,
                AliasFilter.EMPTY,
                1f,
                -1,
                null
            );
        }

        @Override
        public TimeValue scroll() {
            return TimeValue.timeValueMinutes(1);
        }
    }
}
