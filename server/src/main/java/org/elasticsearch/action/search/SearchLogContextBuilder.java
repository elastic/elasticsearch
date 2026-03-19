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
        // response's getTook() is actually in millis, so we get 0 if it took less than 1ms.
        // That's why we have to involve elapsed() here.
        long tookInNanos = Math.max(response.getTook().nanos(), elapsed());
        return new SearchLogContext(task, namedWriteableRegistry, request, tookInNanos, response);
    }

    @Override
    public SearchLogContext build(Exception e) {
        return new SearchLogContext(task, namedWriteableRegistry, request, elapsed(), e);
    }
}
