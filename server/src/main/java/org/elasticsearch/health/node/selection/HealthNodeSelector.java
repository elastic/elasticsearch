/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

/**
 * Main component responsible for selecting the health node of the cluster
 */
public class HealthNodeSelector extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(HealthNodeSelector.class);

    public static final String HEALTH_NODE_SELECTOR_TASK_NAME = "health-node-selector";

    HealthNodeSelector(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
    }

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }
}
