/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

public class TriggeredExecutionContext extends WatchExecutionContext {

    public TriggeredExecutionContext(Watch watch, DateTime executionTime, TriggerEvent triggerEvent, TimeValue defaultThrottlePeriod) {
        super(watch, executionTime, triggerEvent, defaultThrottlePeriod);
    }

    @Override
    public boolean knownWatch() {
        return true;
    }

    @Override
    public final boolean simulateAction(String actionId) {
        return false;
    }

    @Override
    public final boolean skipThrottling(String actionId) {
        return false;
    }

    @Override
    public final boolean recordExecution() {
        return true;
    }
}
