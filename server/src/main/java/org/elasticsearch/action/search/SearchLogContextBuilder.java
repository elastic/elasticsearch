/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.action.ActionLoggerContextBuilder;
import org.elasticsearch.tasks.Task;

public class SearchLogContextBuilder extends ActionLoggerContextBuilder<SearchLogContext, SearchRequest, SearchResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final ProjectState projectState;

    public SearchLogContextBuilder(
        Task task,
        NamedWriteableRegistry namedWriteableRegistry,
        ProjectState projectState,
        SearchRequest request
    ) {
        super(task, request);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.projectState = projectState;
    }

    @Override
    public SearchLogContext build(SearchResponse response) {
        return new SearchLogContext(task, namedWriteableRegistry, projectState, request, response);
    }

    @Override
    public SearchLogContext build(Exception e) {
        return new SearchLogContext(task, namedWriteableRegistry, projectState, request, elapsed(), e);
    }
}
