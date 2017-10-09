/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;

import java.io.IOException;

/**
 * A request to cancel tasks
 */
public class CancelTasksRequest extends BaseTasksRequest<CancelTasksRequest> {

    public static final String DEFAULT_REASON = "by user request";

    private String reason = DEFAULT_REASON;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        reason = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(reason);
    }

    @Override
    public boolean match(Task task) {
        return super.match(task) && task instanceof CancellableTask;
    }

    /**
     * Set the reason for canceling the task.
     */
    public CancelTasksRequest setReason(String reason) {
        this.reason = reason;
        return this;
    }

    /**
     * The reason for canceling the task.
     */
    public String getReason() {
        return reason;
    }
}
