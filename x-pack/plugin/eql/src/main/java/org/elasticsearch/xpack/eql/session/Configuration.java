/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.TaskId;

import java.time.ZoneId;
import java.util.function.Supplier;

public class Configuration extends org.elasticsearch.xpack.ql.session.Configuration {

    private final String[] indices;
    private final TimeValue requestTimeout;
    private final int size;
    private final String clientId;
    private final boolean includeFrozenIndices;
    private final Supplier<Boolean> isCancelled;
    private final TaskId taskId;

    @Nullable
    private final QueryBuilder filter;

    public Configuration(String[] indices, ZoneId zi, String username, String clusterName, QueryBuilder filter, TimeValue requestTimeout,
                         int size, boolean includeFrozen, String clientId, TaskId taskId, Supplier<Boolean> isCancelled) {

        super(zi, username, clusterName);

        this.indices = indices;
        this.filter = filter;
        this.requestTimeout = requestTimeout;
        this.size = size;
        this.clientId = clientId;
        this.includeFrozenIndices = includeFrozen;
        this.taskId = taskId;
        this.isCancelled = isCancelled;
    }

    public String[] indices() {
        return indices;
    }

    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    public QueryBuilder filter() {
        return filter;
    }

    public int size() {
        return size;
    }

    public String clientId() {
        return clientId;
    }

    public boolean includeFrozen() {
        return includeFrozenIndices;
    }

    public boolean isCancelled() {
        return isCancelled.get();
    }

    public TaskId getTaskId() {
        return taskId;
    }
}
