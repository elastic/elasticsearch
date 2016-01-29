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


package org.elasticsearch.tasks;

import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Provider;

/**
 * Current task information
 */
public class Task {

    public static final long NO_PARENT_ID = 0;

    private final long id;

    private final String type;

    private final String action;

    private final Provider<String> description;

    private final String parentNode;

    private final long parentId;


    public Task(long id, String type, String action, Provider<String> description) {
        this(id, type, action, description, null, NO_PARENT_ID);
    }

    public Task(long id, String type, String action, Provider<String> description, String parentNode, long parentId) {
        this.id = id;
        this.type = type;
        this.action = action;
        this.description = description;
        this.parentNode = parentNode;
        this.parentId = parentId;
    }


    public TaskInfo taskInfo(DiscoveryNode node, boolean detailed) {
        return new TaskInfo(node, getId(), getType(), getAction(), detailed ? getDescription() : null, parentNode, parentId);
    }

    /**
     * Returns task id
     */
    public long getId() {
        return id;
    }

    /**
     * Returns task channel type (netty, transport, direct)
     */
    public String getType() {
        return type;
    }

    /**
     * Returns task action
     */
    public String getAction() {
        return action;
    }

    /**
     * Generates task description
     */
    public String getDescription() {
        return description.get();
    }

    /**
     * Returns the parent node of the task or null if the task doesn't have any parent tasks
     */
    public String getParentNode() {
        return parentNode;
    }

    /**
     * Returns id of the parent task or NO_PARENT_ID if the task doesn't have any parent tasks
     */
    public long getParentId() {
        return parentId;
    }

}
