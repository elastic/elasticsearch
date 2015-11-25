/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.action.put;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class PutWatchTests extends AbstractWatcherIntegrationTestCase {
    public void testPut() throws Exception {
        ensureWatcherStarted();

        WatchSourceBuilder source = watchBuilder()
                .trigger(schedule(interval("5m")));

        if (randomBoolean()) {
            source.input(simpleInput());
        }
        if (randomBoolean()) {
            source.condition(alwaysCondition());
        }
        if (randomBoolean()) {
            source.addAction("_action1", loggingAction("{{ctx.watch_id}}"));
        }

        PutWatchResponse response = watcherClient().preparePutWatch("_name").setSource(source).get();

        assertThat(response, notNullValue());
        assertThat(response.isCreated(), is(true));
        assertThat(response.getVersion(), is(1L));
    }

    public void testPutNoTrigger() throws Exception {
        ensureWatcherStarted();
        try {
            watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                    .input(simpleInput())
                    .condition(alwaysCondition())
                    .addAction("_action1", loggingAction("{{ctx.watch_id}}")))
                    .get();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("failed to build watch source. no trigger defined"));
        }
    }

    public void testPutInvalidWatchId() throws Exception {
        ensureWatcherStarted();
        try {
            watcherClient().preparePutWatch("id with whitespaces").setSource(watchBuilder()
                    .trigger(schedule(interval("5m"))))
                    .get();
            fail("Expected ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("Watch id cannot have white spaces"));
        }
    }

    public void testPutInvalidActionId() throws Exception {
        ensureWatcherStarted();
        try {
            watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                    .trigger(schedule(interval("5m")))
                    .addAction("id with whitespaces", loggingAction("{{ctx.watch_id}}")))
                    .get();
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("Action id cannot have white spaces"));
        }
    }
}
