/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import joptsimple.internal.Strings;

import org.elasticsearch.common.logging.activity.ActivityLoggerContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.util.concurrent.TimeUnit;

public class EqlLogContext extends ActivityLoggerContext {
    public static final String TYPE = "eql";
    private final EqlSearchRequest request;
    private final EqlSearchResponse response;

    EqlLogContext(Task task, EqlSearchRequest request, EqlSearchResponse response) {
        super(task, TYPE, TimeUnit.MILLISECONDS.toNanos(response.took()));
        this.request = request;
        this.response = response;
    }

    EqlLogContext(Task task, EqlSearchRequest request, long tookInNanos, Exception error) {
        super(task, TYPE, tookInNanos, error);
        this.request = request;
        this.response = null;
    }

    String getQuery() {
        return request.query();
    }

    public String getIndices() {
        return Strings.join(request.indices(), ",");
    }

    long getHits() {
        if (response == null || response.hits() == null || response.hits().totalHits() == null) {
            return 0;
        }
        return response.hits().totalHits().value();
    }
}
