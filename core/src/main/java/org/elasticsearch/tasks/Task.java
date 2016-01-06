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

    private final long id;

    private final String type;

    private final String action;

    private final Provider<String> description;

    public Task(long id, String type, String action, Provider<String> description) {
        this.id = id;
        this.type = type;
        this.action = action;
        this.description = description;
    }

    public TaskInfo taskInfo(DiscoveryNode node, boolean detailed) {
        return new TaskInfo(node, id, type, action, detailed ? getDescription() : null);
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

}
