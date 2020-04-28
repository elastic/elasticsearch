/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import java.util.Map;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;

class ExecuteEnrichPolicyTask extends Task {

    private volatile ExecuteEnrichPolicyStatus status;

    ExecuteEnrichPolicyTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
    }

    @Override
    public Status getStatus() {
        return status;
    }

    void setStatus(ExecuteEnrichPolicyStatus status) {
        this.status = status;
    }
}
