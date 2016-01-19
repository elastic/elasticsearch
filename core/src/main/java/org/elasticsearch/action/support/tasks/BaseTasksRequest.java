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

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.ChildTask;
import org.elasticsearch.tasks.Task;

import java.io.IOException;

/**
 * A base class for task requests
 */
public class BaseTasksRequest<Request extends BaseTasksRequest<Request>> extends ActionRequest<Request> {


    public static final String[] ALL_ACTIONS = Strings.EMPTY_ARRAY;

    public static final String[] ALL_NODES = Strings.EMPTY_ARRAY;

    public static final long ALL_TASKS = -1L;

    private String[] nodesIds = ALL_NODES;

    private TimeValue timeout;

    private String[] actions = ALL_ACTIONS;

    private String parentNode;

    private long parentTaskId = ALL_TASKS;

    public BaseTasksRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Get information about tasks from nodes based on the nodes ids specified.
     * If none are passed, information for all nodes will be returned.
     */
    public BaseTasksRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    /**
     * Sets the list of action masks for the actions that should be returned
     */
    @SuppressWarnings("unchecked")
    public final Request actions(String... actions) {
        this.actions = actions;
        return (Request) this;
    }

    /**
     * Return the list of action masks for the actions that should be returned
     */
    public String[] actions() {
        return actions;
    }

    public final String[] nodesIds() {
        return nodesIds;
    }

    @SuppressWarnings("unchecked")
    public final Request nodesIds(String... nodesIds) {
        this.nodesIds = nodesIds;
        return (Request) this;
    }

    /**
     * Returns the parent node id that tasks should be filtered by
     */
    public String parentNode() {
        return parentNode;
    }

    @SuppressWarnings("unchecked")
    public Request parentNode(String parentNode) {
        this.parentNode = parentNode;
        return (Request) this;
    }

    /**
     * Returns the parent task id that tasks should be filtered by
     */
    public long parentTaskId() {
        return parentTaskId;
    }

    @SuppressWarnings("unchecked")
    public Request parentTaskId(long parentTaskId) {
        this.parentTaskId = parentTaskId;
        return (Request) this;
    }


    public TimeValue timeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout");
        return (Request) this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodesIds = in.readStringArray();
        actions = in.readStringArray();
        parentNode = in.readOptionalString();
        parentTaskId = in.readLong();
        if (in.readBoolean()) {
            timeout = TimeValue.readTimeValue(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(nodesIds);
        out.writeStringArrayNullable(actions);
        out.writeOptionalString(parentNode);
        out.writeLong(parentTaskId);
        out.writeOptionalStreamable(timeout);
    }

    public boolean match(Task task) {
        if (actions() != null && actions().length > 0 && Regex.simpleMatch(actions(), task.getAction()) == false) {
            return false;
        }
        if (parentNode() != null || parentTaskId() != BaseTasksRequest.ALL_TASKS) {
            if (task instanceof ChildTask) {
                if (parentNode() != null) {
                    if (parentNode().equals(((ChildTask) task).getParentNode()) == false) {
                        return false;
                    }
                }
                if (parentTaskId() != BaseTasksRequest.ALL_TASKS) {
                    if (parentTaskId() != ((ChildTask) task).getParentId()) {
                        return false;
                    }
                }
            } else {
                // This is not a child task and we need to match parent node or id
                return false;
            }
        }
        return true;
    }
}
