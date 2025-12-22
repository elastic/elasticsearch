/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.logging.action.ActionLoggerContextBuilder;
import org.elasticsearch.tasks.Task;

public class SearchActionContextBuilder implements ActionLoggerContextBuilder<SearchActionContext, SearchResponse> {

    private final SearchRequest request;
    private final Task task;

    public SearchActionContextBuilder(Task task, SearchRequest request) {
        this.request = request;
        this.task = task;
    }

    @Override
    public SearchActionContext build(SearchResponse response) {
        return new SearchActionContext(task, request, response);
    }

    @Override
    public SearchActionContext build(Exception e) {
        return new SearchActionContext(task, request, e);
    }
}
