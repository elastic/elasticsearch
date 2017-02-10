/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.activate;


import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ActivateWatchTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        return false;
    }

    // FIXME not to be sleep based
    public void testDeactivateAndActivate() throws Exception {
        WatcherClient watcherClient = watcherClient();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("1s")))
                        .input(simpleInput("foo", "bar"))
                        .addAction("_a1", indexAction("actions", "action1"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));

        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 5);

        // we now know the watch is executing... lets deactivate it
        ActivateWatchResponse activateWatchResponse = watcherClient.prepareActivateWatch("_id", false).get();
        assertThat(activateWatchResponse, notNullValue());
        assertThat(activateWatchResponse.getStatus().state().isActive(), is(false));

        getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(false));

        flush();
        refresh();
        long count1 = docCount(".watcher-history*", "watch_record", matchAllQuery());

        Thread.sleep(10000);

        flush();
        refresh();
        long count2 = docCount(".watcher-history*", "watch_record", matchAllQuery());

        assertThat(count2, is(count1));

        // lets activate it again

        activateWatchResponse = watcherClient.prepareActivateWatch("_id", true).get();
        assertThat(activateWatchResponse, notNullValue());
        assertThat(activateWatchResponse.getStatus().state().isActive(), is(true));

        getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));

        Thread.sleep(10000);
        long count3 = docCount(".watcher-history*", "watch_record", matchAllQuery());
        assertThat(count3, greaterThan(count1));
    }

    public void testLoadWatchWithoutAState() throws Exception {
        WatcherClient watcherClient = watcherClient();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 1 ? 2050"))) // some time in 2050
                        .input(simpleInput("foo", "bar"))
                        .addAction("_a1", indexAction("actions", "action1"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));

        GetResponse getResponse = client().prepareGet(".watches", "watch", "_id").get();
        XContentSource source = new XContentSource(getResponse.getSourceAsBytesRef(), XContentType.JSON);

        Set<String> filters = Sets.newHashSet(
                "trigger.**",
                "input.**",
                "condition.**",
                "throttle_period.**",
                "transform.**",
                "actions.**",
                "metadata.**",
                "_status.version",
                "_status.last_checked",
                "_status.last_met_condition",
                "_status.actions.**");

        XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), new BytesStreamOutput(), filters);
        source.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // now that we filtered out the watch status state, lets put it back in
        IndexResponse indexResponse = client().prepareIndex(".watches", "watch", "_id")
                .setSource(builder.bytes(), XContentType.JSON)
                .get();
        assertThat(indexResponse.getId(), is("_id"));

        // now, let's restart
        assertThat(watcherClient.prepareWatchService().stop().get().isAcknowledged(), is(true));
        ensureWatcherStopped();
        assertThat(watcherClient.prepareWatchService().start().get().isAcknowledged(), is(true));
        ensureWatcherStarted();

        getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state(), notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));
    }
}
