/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * What the fetch operators of one query need on its coordinator, beyond the plan.
 *
 * @param rootTask       the task of the query. Every fetch request is its child, so cancelling the query cancels them.
 * @param completionInfo acquires a listener for the drivers of one fetch request. The query adds them to its own
 *                       counters and profile.
 * @param fetchFailures  the shards whose fetch failed, which the query reports once its computes are done
 * @param fetchContexts  the contexts the data nodes keep for the fetch of this execution, which it frees as soon as its cut
 *                       and its fetch no longer read them
 */
public record QueryFetchScope(
    CancellableTask rootTask,
    String sessionId,
    Configuration configuration,
    Supplier<ActionListener<DriverCompletionInfo>> completionInfo,
    FetchFailures fetchFailures,
    FetchContextLease.Execution fetchContexts
) {
    public QueryFetchScope {
        Objects.requireNonNull(rootTask, "rootTask");
        Objects.requireNonNull(completionInfo, "completionInfo");
        Objects.requireNonNull(fetchFailures, "fetchFailures");
        Objects.requireNonNull(fetchContexts, "fetchContexts");
    }
}
