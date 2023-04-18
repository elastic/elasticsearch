/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;

public class SearchContextSourcePrinter {
    private final SearchContext searchContext;

    public SearchContextSourcePrinter(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(searchContext.indexShard().shardId());
        builder.append(" ");
        if (searchContext.request() != null && searchContext.request().source() != null) {
            builder.append("source[").append(searchContext.request().source().toString()).append("], ");
        } else {
            builder.append("source[], ");
        }
        if (searchContext.getTask() != null && searchContext.getTask().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER) != null) {
            builder.append("id[").append(searchContext.getTask().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER)).append("], ");
        } else {
            builder.append("id[], ");
        }
        return builder.toString();
    }
}
