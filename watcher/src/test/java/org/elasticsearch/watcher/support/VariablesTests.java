/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTime;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.watcher.test.WatcherTestUtils.assertValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.joda.time.DateTimeZone.UTC;

/**
 *
 */
public class VariablesTests extends ESTestCase {
    public void testCreateCtxModel() throws Exception {
        DateTime scheduledTime = DateTime.now(UTC);
        DateTime triggeredTime = scheduledTime.plusMillis(50);
        DateTime executionTime = triggeredTime.plusMillis(50);
        Payload payload = new Payload.Simple(singletonMap("payload_key", "payload_value"));
        Map<String, Object> metatdata = singletonMap("metadata_key", "metadata_value");
        TriggerEvent event = new ScheduleTriggerEvent("_watch_id", triggeredTime, scheduledTime);
        Wid wid = new Wid("_watch_id", 0, executionTime);
        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContextBuilder("_watch_id")
                .wid(wid)
                .executionTime(executionTime)
                .triggerEvent(event)
                .payload(payload)
                .metadata(metatdata)
                .buildMock();

        Map<String, Object> model = Variables.createCtxModel(ctx, payload);
        assertThat(model, notNullValue());
        assertThat(model.size(), is(1));
        assertValue(model, "ctx", instanceOf(Map.class));
        assertValue(model, "ctx.id", is(wid.value()));
        assertValue(model, "ctx.execution_time", is(executionTime));
        assertValue(model, "ctx.trigger", is(event.data()));
        assertValue(model, "ctx.payload", is(payload.data()));
        assertValue(model, "ctx.metadata", is(metatdata));
    }
}
