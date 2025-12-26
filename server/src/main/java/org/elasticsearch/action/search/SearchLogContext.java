/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import joptsimple.internal.Strings;

import org.elasticsearch.common.logging.action.ActionLoggerContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

public class SearchLogContext extends ActionLoggerContext {
    private static final String TYPE = "search";
    private final SearchRequest request;
    private final @Nullable SearchResponse response;

    public SearchLogContext(Task task, SearchRequest request, SearchResponse response) {
        super(task, TYPE, response.getTook().nanos());
        this.request = request;
        this.response = response;
    }

    SearchLogContext(Task task, SearchRequest request, Exception error) {
        super(task, TYPE, error);
        this.request = request;
        this.response = null;
    }

    String getQuery() {
        var source = request.source();
        if (source == null) {
            return "{}";
        } else {
            return escapeJson(source.toString(FORMAT_PARAMS));
        }
    }

    long getHits() {
        if (response == null || response.getHits() == null || response.getHits().getTotalHits() == null) {
            return 0;
        }
        return response.getHits().getTotalHits().value();
    }

    public String getIndices() {
        return Strings.join(request.indices(), ",");
    }
}
