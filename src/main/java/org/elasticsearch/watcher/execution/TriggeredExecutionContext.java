/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.watch.Watch;

/**
 */
public class TriggeredExecutionContext extends WatchExecutionContext {

    public TriggeredExecutionContext(Watch watch, DateTime executionTime, TriggerEvent triggerEvent) {
        super(watch, executionTime, triggerEvent);
    }

    @Override
    final public boolean simulateAction(String actionId) {
        return false;
    }

    @Override
    final public boolean recordExecution() {
        return true;
    }
}
