/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.action.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchRequest;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;
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
public class DeleteWatchTests extends AbstractWatcherIntegrationTestCase {
    public void testDelete() throws Exception {
        ensureWatcherStarted();
        PutWatchResponse putResponse = watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                .trigger(schedule(interval("5m")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .addAction("_action1", loggingAction("{{ctx.watch_id}}")))
                .get();

        assertThat(putResponse, notNullValue());
        assertThat(putResponse.isCreated(), is(true));

        DeleteWatchResponse deleteResponse = watcherClient().deleteWatch(new DeleteWatchRequest("_name")).get();
        assertThat(deleteResponse, notNullValue());
        assertThat(deleteResponse.getId(), is("_name"));
        assertThat(deleteResponse.getVersion(), is(putResponse.getVersion() + 1));
        assertThat(deleteResponse.isFound(), is(true));
    }

    public void testDeleteNotFound() throws Exception {
        DeleteWatchResponse response = watcherClient().deleteWatch(new DeleteWatchRequest("_name")).get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_name"));
        assertThat(response.getVersion(), is(1L));
        assertThat(response.isFound(), is(false));
    }

    public void testDeleteInvalidWatchId() throws Exception {
        try {
            watcherClient().deleteWatch(new DeleteWatchRequest("id with whitespaces")).actionGet();
            fail("Expected ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("Watch id cannot have white spaces"));
        }
    }
}
