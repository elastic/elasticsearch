/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class XPackUsageRequest extends LocalClusterStateRequest {

    public XPackUsageRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    public XPackUsageRequest(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
