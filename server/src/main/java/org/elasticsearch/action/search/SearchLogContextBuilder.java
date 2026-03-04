/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.activity.ActivityLoggerContextBuilder;
import org.elasticsearch.tasks.Task;

public class SearchLogContextBuilder extends ActivityLoggerContextBuilder<SearchLogContext, SearchRequest, SearchResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;

    public SearchLogContextBuilder(Task task, NamedWriteableRegistry namedWriteableRegistry, SearchRequest request) {
        super(task, request);
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public SearchLogContext build(SearchResponse response) {
        return new SearchLogContext(task, namedWriteableRegistry, request, response);
    }

    @Override
    public SearchLogContext build(Exception e) {
        return new SearchLogContext(task, namedWriteableRegistry, request, elapsed(), e);
    }
}
