/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.watcher.execution.ActionExecutionMode;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.junit.Test;

/**
 *
 */
public class WatchExecuteTests  extends AbstractWatcherIntegrationTests {


    @Test(expected = ActionRequestValidationException.class)
    public void testExecute_InvalidWatchId() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        watcherClient().prepareExecuteWatch("id with whitespaces")
                .setTriggerEvent(new ScheduleTriggerEvent(now, now))
                .get();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testExecute_InvalidActionId() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        watcherClient().prepareExecuteWatch("_id")
                .setTriggerEvent(new ScheduleTriggerEvent(now, now))
                .setActionMode("id with whitespaces", randomFrom(ActionExecutionMode.values()))
                .get();
    }
}
