/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;

import java.util.Objects;

public class SpanId {
    private final String rawId;

    private SpanId(String rawId) {
        this.rawId = Objects.requireNonNull(rawId);
    }

    public String getRawId() {
        return rawId;
    }

    @Override
    public String toString() {
        return "SpanId[" + rawId + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpanId spanId = (SpanId) o;
        return rawId.equals(spanId.rawId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawId);
    }

    public static SpanId forTask(Task task) {
        return new SpanId("task-" + task.getId());
    }

    public static SpanId forRestRequest(RestRequest restRequest) {
        return new SpanId("rest-" + restRequest.getRequestId());
    }

    public static SpanId forBareString(String rawId) {
        return new SpanId(rawId);
    }
}
