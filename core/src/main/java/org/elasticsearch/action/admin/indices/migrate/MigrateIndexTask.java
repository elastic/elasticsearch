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

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.admin.indices.migrate.TransportMigrateIndexAction.Operation;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

/**
 * Task subclass that supports getting information about the currently running migration.
 */
public class MigrateIndexTask extends Task {
    private volatile Operation operation;

    public MigrateIndexTask(long id, String type, String action, String description, TaskId parentTask) {
        super(id, type, action, description, parentTask);
    }

    /**
     * Set the operation that is performing this task. It is set once when the task starts and remains constant for the lifetime of the
     * task.
     */
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /**
     * The operation that is performing this task. It'll be null before the task is properly started. 
     */
    Operation getOperation() {
        return operation;
    }
}
