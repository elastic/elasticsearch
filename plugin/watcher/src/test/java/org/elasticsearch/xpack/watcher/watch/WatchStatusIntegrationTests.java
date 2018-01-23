/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval.Unit.SECONDS;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class WatchStatusIntegrationTests extends AbstractWatcherIntegrationTestCase {

    public void testThatStatusGetsUpdated() {
        WatcherClient watcherClient = watcherClient();
        watcherClient.preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, SECONDS)))
                        .input(simpleInput())
                        .condition(NeverCondition.INSTANCE)
                        .addAction("_logger", loggingAction("logged text")))
                .get();
        timeWarp().trigger("_name");

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch().setId("_name").get();
        assertThat(getWatchResponse.isFound(), is(true));
        assertThat(getWatchResponse.getSource(), notNullValue());
        assertThat(getWatchResponse.getStatus().lastChecked(), is(notNullValue()));

        GetResponse getResponse = client().prepareGet(".watches", "doc", "_name").get();
        getResponse.getSource();
        XContentSource source = new XContentSource(getResponse.getSourceAsBytesRef(), XContentType.JSON);
        String lastChecked = source.getValue("status.last_checked");

        assertThat(lastChecked, is(notNullValue()));
        assertThat(getWatchResponse.getStatus().lastChecked().toString(), is(lastChecked));
    }

}
