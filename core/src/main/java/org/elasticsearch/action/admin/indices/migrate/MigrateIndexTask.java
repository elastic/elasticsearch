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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.migrate.TransportMigrateIndexAction.DocumentMigrater;
import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class MigrateIndexTask extends Task {
    private final MigrateIndexRequest request;
    private ActionListener<MigrateIndexResponse> listener;
    private List<MigrateIndexTask> duplicates;
    private MigrateIndexTask waitingFor;

    public MigrateIndexTask(long id, String type, String action, String description, TaskId parentTask, MigrateIndexRequest request) {
        super(id, type, action, description, parentTask);
        this.request = request;
    }

    // NOCOMMIT be much more careful about reading and writing these and threads
    /**
     * The request that this task is trying to fulfill.
     */
    public MigrateIndexRequest getRequest() {
        return request;
    }

    /**
     * The listener that is waiting on this task to complete.
     */
    public synchronized ActionListener<MigrateIndexResponse> getListener() {
        return listener;
    }    

    /**
     * Set the listener that is waiting on this task to complete.
     */
    public synchronized void setListener(ActionListener<MigrateIndexResponse> listener) {
        this.listener = listener;
    }

    /**
     * Setup a duplicate of this task that will wait for this task to complete.
     */
    public synchronized void addDuplicate(MigrateIndexTask duplicate) {
        if (duplicates == null) {
            duplicates = new ArrayList<>();
        }
        duplicates.add(duplicate);
        synchronized (duplicate) {
            duplicate.waitingFor = this;
        }
    }

    /**
     * Tasks that are attempting to duplicate the effort of this task. Instead of duplicating the effort they instead block while this task
     * is running. While this method is synchronized so it'll return a consistent copy of the duplicates list, all modification to the list
     * is done by first synchronizing on TransportMigrateIndexAction#runningTasks and then synchronizing on this object.
     */
    public synchronized List<MigrateIndexTask> getDuplicates() {
        return duplicates == null ? emptyList() : new ArrayList<>(duplicates);
    }

    /**
     * The task that this is waiting for.
     */
    public synchronized MigrateIndexTask getWaitingFor() {
       return waitingFor; 
    }
}
