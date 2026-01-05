/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.Task;

import java.io.IOException;

public class ListReindexRequest extends BaseTasksRequest<ListReindexRequest> {

    public ListReindexRequest() {
        super();
    }

    public ListReindexRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public boolean match(Task task) {
        if (super.match(task) == false) {
            return false;
        }

        // Filter for only reindex tasks
        if (ReindexAction.NAME.equals(task.getAction()) == false) {
            return false;
        }

        // Filter out subtasks
        if (task.getParentTaskId().isSet()) {
            return false;
        }
        return true;
    }
}
