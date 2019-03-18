/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

public class SnapshotLifecycleTask implements SchedulerEngine.Listener {

    private static Logger logger = LogManager.getLogger(SnapshotLifecycleTask.class);

    private final Client client;

    public SnapshotLifecycleTask(final Client client) {
        this.client = client;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        logger.info("--> triggered job: {}", event);
        // TODO: implement snapshotting the indices from the job
    }
}
