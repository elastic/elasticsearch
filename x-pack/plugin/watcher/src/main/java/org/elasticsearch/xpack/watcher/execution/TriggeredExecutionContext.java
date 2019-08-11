/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;

import java.time.ZonedDateTime;

public class TriggeredExecutionContext extends WatchExecutionContext {

    private final boolean overrideOnConflict;

    public TriggeredExecutionContext(String watchId, ZonedDateTime executionTime, TriggerEvent triggerEvent,
                                     TimeValue defaultThrottlePeriod) {
        this(watchId, executionTime, triggerEvent, defaultThrottlePeriod, false);
    }

    TriggeredExecutionContext(String watchId, ZonedDateTime executionTime, TriggerEvent triggerEvent, TimeValue defaultThrottlePeriod,
                              boolean overrideOnConflict) {
        super(watchId, executionTime, triggerEvent, defaultThrottlePeriod);
        this.overrideOnConflict = overrideOnConflict;
    }

    @Override
    public boolean overrideRecordOnConflict() {
        return overrideOnConflict;
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
    public boolean shouldBeExecuted() {
        return watch().status().state().isActive();
    }

    @Override
    public final boolean recordExecution() {
        return true;
    }
}
