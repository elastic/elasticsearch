/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;

import java.time.ZoneId;
import java.util.List;

public class EqlConfiguration extends org.elasticsearch.xpack.ql.session.Configuration {

    private final String[] indices;
    private final TimeValue requestTimeout;
    private final String clientId;
    private final IndicesOptions indicesOptions;
    private final TaskId taskId;
    private final EqlSearchTask task;
    private final int fetchSize;

    @Nullable
    private final QueryBuilder filter;
    @Nullable
    private final List<FieldAndFormat> fetchFields;

    public EqlConfiguration(String[] indices, ZoneId zi, String username, String clusterName, QueryBuilder filter,
                            List<FieldAndFormat> fetchFields, TimeValue requestTimeout, IndicesOptions indicesOptions, int fetchSize,
                            String clientId, TaskId taskId, EqlSearchTask task) {
        super(zi, username, clusterName);

        this.indices = indices;
        this.filter = filter;
        this.fetchFields = fetchFields;
        this.requestTimeout = requestTimeout;
        this.clientId = clientId;
        this.indicesOptions = indicesOptions;
        this.taskId = taskId;
        this.task = task;
        this.fetchSize = fetchSize;
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

    public int fetchSize() {
        return fetchSize;
    }

    public QueryBuilder filter() {
        return filter;
    }

    public List<FieldAndFormat> fetchFields() {
        return fetchFields;
    }

    public String clientId() {
        return clientId;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public boolean isCancelled() {
        return task.isCancelled();
    }

    public TaskId getTaskId() {
        return taskId;
    }
}
