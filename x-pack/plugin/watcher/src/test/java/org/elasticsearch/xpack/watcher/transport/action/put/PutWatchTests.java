/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.put;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class PutWatchTests extends AbstractWatcherIntegrationTestCase {
    public void testPut() throws Exception {
        WatchSourceBuilder source = watchBuilder()
                .trigger(schedule(interval("5m")));

        if (randomBoolean()) {
            source.input(simpleInput());
        }
        if (randomBoolean()) {
            source.condition(AlwaysCondition.INSTANCE);
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
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                .input(simpleInput())
                .condition(AlwaysCondition.INSTANCE)
                .addAction("_action1", loggingAction("{{ctx.watch_id}}")))
                .get());
        assertEquals("Failed to build ToXContent", exception.getMessage());
    }
}
