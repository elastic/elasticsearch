/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

/**
 * Task that returns additional state information
 */
public class PersistentTask extends CancellableTask {
    private Provider<Status> statusProvider;

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
}