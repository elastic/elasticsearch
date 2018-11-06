/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.joda.time.DateTimeZone.UTC;

public class VariablesTests extends ESTestCase {
    public void testCreateCtxModel() throws Exception {
        DateTime scheduledTime = DateTime.now(UTC);
        DateTime triggeredTime = scheduledTime.plusMillis(50);
        DateTime executionTime = triggeredTime.plusMillis(50);
        Payload payload = new Payload.Simple(singletonMap("payload_key", "payload_value"));
        Map<String, Object> metatdata = singletonMap("metadata_key", "metadata_value");
        TriggerEvent event = new ScheduleTriggerEvent("_watch_id", triggeredTime, scheduledTime);
        Wid wid = new Wid("_watch_id", executionTime);
        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContextBuilder("_watch_id")
                .wid(wid)
                .executionTime(executionTime)
                .triggerEvent(event)
                .payload(payload)
                .metadata(metatdata)
                .buildMock();

        Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);
        assertThat(model, notNullValue());
        assertThat(model.size(), is(1));

        assertThat(ObjectPath.eval("ctx", model), instanceOf(Map.class));
        assertThat(ObjectPath.eval("ctx.id", model), is(wid.value()));
        assertThat(ObjectPath.eval("ctx.execution_time", model), is(executionTime));
        assertThat(ObjectPath.eval("ctx.trigger", model), is(event.data()));
        assertThat(ObjectPath.eval("ctx.payload", model), is(payload.data()));
        assertThat(ObjectPath.eval("ctx.metadata", model), is(metatdata));
    }
}
