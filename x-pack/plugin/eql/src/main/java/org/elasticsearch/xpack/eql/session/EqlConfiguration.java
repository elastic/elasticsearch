/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;

import java.time.ZoneId;

public class EqlConfiguration extends org.elasticsearch.xpack.ql.session.Configuration {

    private final String[] indices;
    private final TimeValue requestTimeout;
    private final int size;
    private final String clientId;
    private final boolean includeFrozenIndices;
    private final TaskId taskId;
    private final EqlSearchTask task;
    private final boolean isCaseSensitive;

    @Nullable
    private final QueryBuilder filter;

    public EqlConfiguration(String[] indices, ZoneId zi, String username, String clusterName, QueryBuilder filter, TimeValue requestTimeout,
                         int size, boolean includeFrozen, boolean isCaseSensitive, String clientId, TaskId taskId, EqlSearchTask task) {
        super(zi, username, clusterName);

        this.indices = indices;
        this.filter = filter;
        this.requestTimeout = requestTimeout;
        this.size = size;
        this.clientId = clientId;
        this.includeFrozenIndices = includeFrozen;
        this.isCaseSensitive = isCaseSensitive;
        this.taskId = taskId;
        this.task = task;
    }

    public String[] indices() {
        return indices;
    }

    public String indexAsWildcard() {
        return Strings.arrayToCommaDelimitedString(indices);
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

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    public boolean isCancelled() {
        return task.isCancelled();
    }

    public TaskId getTaskId() {
        return taskId;
    }
}
