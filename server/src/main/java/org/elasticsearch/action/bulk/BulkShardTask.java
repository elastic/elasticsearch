/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class BulkShardTask extends CancellableTask {

    private static final Logger logger = LogManager.getLogger(BulkShardTask.class);
    private final AtomicReference<State> state;

    public BulkShardTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
        this.state = new AtomicReference<>(State.STARTED);
    }

    @Override
    public Status getStatus() {
        return null;
    }

    public enum State {
        STARTED,
        ABORTED,
        COMPLETED
    }

    @Override
    public void onCancelled() {
        // Resource release
    }

}
