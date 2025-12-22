/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonStringEncoder;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public abstract class ActionLoggerContext {
    private final long tookInNanos;
    private final boolean success;
    private final String type;
    private final @Nullable Exception error;
    private final Task task;

    public static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    public static String escapeJson(String text) {
        byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
        return new String(sourceEscaped, StandardCharsets.UTF_8);
    }

    public ActionLoggerContext(Task task, String type, Exception error) {
        this.type = type;
        this.success = false;
        this.error = error;
        this.tookInNanos = 0;
        this.task = task;
    }

    public ActionLoggerContext(Task task, String type, long tookInNanos) {
        this.type = type;
        this.success = true;
        this.tookInNanos = tookInNanos;
        this.error = null;
        this.task = task;
    }

    public long getTookInNanos() {
        return tookInNanos;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getType() {
        return type;
    }

    public String getErrorMessage() {
        return error == null ? "" : error.getMessage();
    }

    public String getErrorType() {
        return error == null ? "" : error.getClass().getSimpleName();
    }

    public String getTaskId() {
        return task.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
    }
}
