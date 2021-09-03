/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;

import java.util.Map;

public class ExecuteEnrichPolicyTask extends Task {

    private volatile ExecuteEnrichPolicyStatus status;

    public ExecuteEnrichPolicyTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTask, headers);
    }

    @Override
    public Status getStatus() {
        return status;
    }

    public void setStatus(ExecuteEnrichPolicyStatus status) {
        this.status = status;
    }
}
