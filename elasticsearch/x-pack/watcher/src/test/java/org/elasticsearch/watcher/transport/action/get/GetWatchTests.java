/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.action.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

import java.util.Map;

import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class GetWatchTests extends AbstractWatcherIntegrationTestCase {
    public void testGet() throws Exception {
        PutWatchResponse putResponse = watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                .trigger(schedule(interval("5m")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .addAction("_action1", loggingAction("{{ctx.watch_id}}")))
                .get();

        assertThat(putResponse, notNullValue());
        assertThat(putResponse.isCreated(), is(true));

        GetWatchResponse getResponse = watcherClient().getWatch(new GetWatchRequest("_name")).get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.getId(), is("_name"));
        assertThat(getResponse.getStatus().version(), is(putResponse.getVersion()));
        Map<String, Object> source = getResponse.getSource().getAsMap();
        assertThat(source, notNullValue());
        assertThat(source, hasKey("trigger"));
        assertThat(source, hasKey("input"));
        assertThat(source, hasKey("condition"));
        assertThat(source, hasKey("actions"));
        assertThat(source, not(hasKey("status")));
    }

    public void testGetInvalidWatchId() throws Exception {
        try {
            watcherClient().prepareGetWatch("id with whitespaces").get();
            fail("Expected ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("Watch id cannot have white spaces"));
        }
    }

    public void testGetNotFound() throws Exception {
        GetWatchResponse getResponse = watcherClient().getWatch(new GetWatchRequest("_name")).get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getId(), is("_name"));
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.getStatus(), nullValue());
        assertThat(getResponse.getSource(), nullValue());
        XContentSource source = getResponse.getSource();
        assertThat(source, nullValue());
    }
}
