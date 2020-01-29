/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;
import static org.elasticsearch.threadpool.ThreadPool.Names.SAME;
import static org.elasticsearch.xpack.search.AsyncSearchStoreService.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.search.AsyncSearchReaperExecutor.Params;
import static org.elasticsearch.xpack.search.AsyncSearchStoreService.IS_RUNNING_FIELD;
import static org.elasticsearch.xpack.search.AsyncSearchTemplateRegistry.ASYNC_SEARCH_ALIAS;

public class AsyncSearchReaperExecutor extends PersistentTasksExecutor<Params> {
    public static final String NAME = "async_search/reaper";

    private final Client client;
    private final ThreadPool threadPool;

    public AsyncSearchReaperExecutor(Client client, ThreadPool threadPool) {
        super(NAME, SAME);
        this.client = new OriginSettingClient(client, TASKS_ORIGIN);
        this.threadPool = threadPool;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, Params params, PersistentTaskState state) {
        cleanup(task, client, 100, System.currentTimeMillis());
    }

    public void cleanup(AllocatedPersistentTask task, Client client, int pageSize, long nowInMillis) {
        SearchRequest request = new SearchRequest(ASYNC_SEARCH_ALIAS);
        request.source().query(QueryBuilders.rangeQuery(EXPIRATION_TIME_FIELD).gt(nowInMillis));
        request.scroll(TimeValue.timeValueMinutes(1));
        client.search(request, nextPageListener(task, pageSize, () -> {
            if (task.isCancelled() == false) {
                threadPool.schedule(() -> cleanup(task, client, pageSize, System.currentTimeMillis()),
                    TimeValue.timeValueMinutes(30), GENERIC);
            }
        }));
    }

    private void nextPage(AllocatedPersistentTask task, Client client, String scrollId, int pageSize, Runnable onCompletion) {
        if (task.isCancelled()) {
            onCompletion.run();
            return;
        }
        client.searchScroll(new SearchScrollRequest(scrollId).scroll(TimeValue.timeValueMinutes(1)),
            nextPageListener(task, pageSize, onCompletion));
    }

    public void deleteAll(Client client, Collection<String> toCancel, Collection<String> toDelete, ActionListener<Void> listener) {
        if (toDelete.isEmpty() && toCancel.isEmpty()) {
            listener.onResponse(null);
        }
        CountDown counter = new CountDown(toDelete.size() + 1);
        BulkRequest bulkDelete = new BulkRequest();
        for (String id : toDelete) {
            bulkDelete.add(new DeleteRequest(ASYNC_SEARCH_ALIAS).id(id));
        }
        client.bulk(bulkDelete, ActionListener.wrap(() -> {
            if (counter.countDown()) {
                listener.onResponse(null);
            }
        }));
        for (String id : toCancel) {
            ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
                threadContext.markAsSystemContext();
                client.execute(DeleteAsyncSearchAction.INSTANCE, new DeleteAsyncSearchAction.Request(id),
                    ActionListener.wrap(() -> {
                        if (counter.countDown()) {
                            listener.onResponse(null);
                        }
                    }));
            }
        }
    }

    private ActionListener<SearchResponse> nextPageListener(AllocatedPersistentTask task, int pageSize, Runnable onCompletion) {
        return new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse response) {
                if (task.isCancelled()) {
                    onCompletion.run();
                    return;
                }
                final List<String> toDelete = new ArrayList<>();
                final List<String> toCancel = new ArrayList<>();
                for (SearchHit hit : response.getHits()) {
                    boolean isRunning = (boolean) hit.getSourceAsMap().get(IS_RUNNING_FIELD);
                    if (isRunning) {
                        toCancel.add(hit.getId());
                    } else {
                        toDelete.add(hit.getId());
                    }
                }
                deleteAll(client, toDelete, toCancel, ActionListener.wrap(() -> {
                    if (response.getHits().getHits().length < pageSize) {
                        onCompletion.run();
                    } else {
                        nextPage(task, client, response.getScrollId(), pageSize, onCompletion);
                    }
                }));
            }

            @Override
            public void onFailure(Exception exc) {
                onCompletion.run();
            }
        };
    }

    static class Params implements PersistentTaskParams {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) {
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }
    }
}
