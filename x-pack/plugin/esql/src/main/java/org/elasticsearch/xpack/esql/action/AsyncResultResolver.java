/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LoadResult;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves LoadResult logical plans by fetching schema from async query results.
 */
public class AsyncResultResolver {

    private final Client client;
    private final ThreadPool threadPool;

    public AsyncResultResolver(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * Resolves all LoadResult nodes in the plan by fetching their schemas from async results.
     */
    public void resolveLoadResults(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        List<LoadResult> loadResults = new ArrayList<>();
        plan.forEachUp(LoadResult.class, loadResults::add);

        if (loadResults.isEmpty()) {
            listener.onResponse(plan);
            return;
        }

        // For now, we'll process them serially. If needed, we can parallelize later.
        resolveLoadResultsSequentially(plan, loadResults.iterator(), listener);
    }

    private void resolveLoadResultsSequentially(
        LogicalPlan plan,
        java.util.Iterator<LoadResult> iterator,
        ActionListener<LogicalPlan> listener
    ) {
        if (iterator.hasNext() == false) {
            listener.onResponse(plan);
            return;
        }

        LoadResult loadResult = iterator.next();
        if (loadResult.resolved()) {
            // Already resolved, skip
            resolveLoadResultsSequentially(plan, iterator, listener);
            return;
        }

        String searchId = BytesRefs.toString(loadResult.searchId().fold(FoldContext.small()));
        GetAsyncResultRequest request = new GetAsyncResultRequest(searchId);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1)); // Don't wait, just get current state

        // Fork back to SEARCH threadpool since async GET runs on a different threadpool
        var forkingListener = ActionListener.wrap(
            (EsqlQueryResponse response) -> threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                try {
                    List<Attribute> schema = response.columns()
                        .stream()
                        .map(col -> (Attribute) new ReferenceAttribute(
                            loadResult.source(),
                            col.name(),
                            DataType.fromEs(col.outputType())
                        ))
                        .collect(Collectors.toList());

                    LogicalPlan updatedPlan = plan.transformUp(LoadResult.class, lr -> {
                        if (lr.searchId().equals(loadResult.searchId())) {
                            return lr.withOutput(schema);
                        }
                        return lr;
                    });

                    resolveLoadResultsSequentially(updatedPlan, iterator, listener);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }),
            (Exception e) -> threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                listener.onFailure(
                    new IllegalArgumentException("Failed to load async result [" + searchId + "]: " + e.getMessage(), e)
                );
            })
        );

        client.execute(EsqlAsyncGetResultAction.INSTANCE, request, forkingListener);
    }
}

