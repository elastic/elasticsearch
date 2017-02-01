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
package org.elasticsearch.persistent;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

/**
 * Task that returns additional state information
 */
public class PersistentTask extends CancellableTask {
    private Provider<Status> statusProvider;

    private long persistentTaskId;

    public PersistentTask(long id, String type, String action, String description, TaskId parentTask) {
        super(id, type, action, description, parentTask);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    @Override
    public Status getStatus() {
        Provider<Status> statusProvider = this.statusProvider;
        if (statusProvider != null) {
            return statusProvider.get();
        } else {
            return null;
        }
    }

    public void setStatusProvider(Provider<Status> statusProvider) {
        assert this.statusProvider == null;
        this.statusProvider = statusProvider;
    }

    public long getPersistentTaskId() {
        return persistentTaskId;
    }

    public void setPersistentTaskId(long persistentTaskId) {
        this.persistentTaskId = persistentTaskId;
    }
}
