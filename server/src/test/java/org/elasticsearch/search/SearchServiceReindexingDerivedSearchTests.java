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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@link SearchService} reindex-derived detection (via {@link ReaderContext#openedUnderReindexingTask()})
 * using a real node {@link SearchService} and {@link TaskManager}. These are not in {@link SearchServiceTests}
 * because that class extends {@link org.elasticsearch.index.shard.IndexShardTestCase} and does not build a
 * {@link SearchService} wired to task management.
 */
public class SearchServiceReindexingDerivedSearchTests extends ESSingleNodeTestCase {

    /**
     * A reindex or resume-reindex task may be an ancestor two hops above the shard search task.
     */
    public void testOpenedUnderReindexingTaskWhenReindexAncestorIsTwoHopsAway() {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final TaskManager taskManager = getInstanceFromNode(TransportService.class).getTaskManager();
        final String localNode = clusterService.localNode().getId();

        final Task reindexTask = taskManager.register("test", ReindexAction.NAME, emptyParentTaskRequest());
        final TaskId reindexTaskId = new TaskId(localNode, reindexTask.getId());
        final Task middleTask = taskManager.register("test", "internal:test_middle", parentFixedTaskRequest(reindexTaskId));
        final TaskId middleTaskId = new TaskId(localNode, middleTask.getId());
        final SearchShardTask shardTask = new SearchShardTask(
            randomNonNegativeLong(),
            "test",
            "indices:data/read/search[phase/query]",
            "shard",
            middleTaskId,
            Map.of()
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            pitRequest(indexShard),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            TimeValue.timeValueMinutes(5).millis(),
            shardTask
        );
        try {
            assertTrue(context.openedUnderReindexingTask());
        } finally {
            unregisterAncestors(taskManager, shardTask);
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * Without a {@link SearchShardTask}, the reader context is not marked as reindex-derived.
     */
    public void testOpenedUnderReindexingTaskFalseWhenSearchShardTaskNull() {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final ReaderContext context = searchService.createAndPutReaderContext(
            pitRequest(indexShard),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            TimeValue.timeValueMinutes(5).millis(),
            null
        );
        try {
            assertFalse(context.openedUnderReindexingTask());
        } finally {
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * Parents registered on another node are not walked; detection stays false.
     */
    public void testOpenedUnderReindexingTaskFalseWhenParentTaskOnDifferentNode() {
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
            new TaskId("other_node", 1L),
            Map.of()
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            pitRequest(indexShard),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            TimeValue.timeValueMinutes(5).millis(),
            shardTask
        );
        try {
            assertFalse(context.openedUnderReindexingTask());
        } finally {
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * If the parent task id is not present in the local {@link TaskManager}, the chain stops and detection is false.
     */
    public void testOpenedUnderReindexingTaskFalseWhenParentTaskNotRegistered() {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final String localNode = clusterService.localNode().getId();
        final SearchShardTask shardTask = new SearchShardTask(
            randomNonNegativeLong(),
            "test",
            "indices:data/read/search[phase/query]",
            "shard",
            new TaskId(localNode, Long.MAX_VALUE),
            Map.of()
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            pitRequest(indexShard),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            TimeValue.timeValueMinutes(5).millis(),
            shardTask
        );
        try {
            assertFalse(context.openedUnderReindexingTask());
        } finally {
            searchService.freeReaderContext(context.id());
        }
    }

    /**
     * A local parent that is not reindex or resume-reindex yields false even when registered.
     */
    public void testOpenedUnderReindexingTaskFalseWhenAncestorIsNotReindex() {
        createIndex("index");
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final TaskManager taskManager = getInstanceFromNode(TransportService.class).getTaskManager();
        final String localNode = clusterService.localNode().getId();

        final Task otherTask = taskManager.register("test", "cluster:monitor/state", emptyParentTaskRequest());
        final TaskId otherId = new TaskId(localNode, otherTask.getId());
        final SearchShardTask shardTask = new SearchShardTask(
            randomNonNegativeLong(),
            "test",
            "indices:data/read/search[phase/query]",
            "shard",
            otherId,
            Map.of()
        );
        final ReaderContext context = searchService.createAndPutReaderContext(
            pitRequest(indexShard),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            TimeValue.timeValueMinutes(5).millis(),
            shardTask
        );
        try {
            assertFalse(context.openedUnderReindexingTask());
        } finally {
            taskManager.unregister(otherTask);
            searchService.freeReaderContext(context.id());
        }
    }

    private static ShardSearchRequest pitRequest(IndexShard indexShard) {
        return new ShardSearchRequest(
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
    }

    private static TaskAwareRequest emptyParentTaskRequest() {
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

    private static TaskAwareRequest parentFixedTaskRequest(TaskId parentTaskId) {
        return new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return parentTaskId;
            }
        };
    }

    private static void unregisterAncestors(TaskManager taskManager, SearchShardTask shardTask) {
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
}
