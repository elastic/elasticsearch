/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.transport.action.activate;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.List;
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

    public void testDeactivateAndActivate() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client())
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("1s")))
                        .input(simpleInput("foo", "bar"))
                        .addAction("_a1", indexAction("actions"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client(), "_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));

        logger.info("Waiting for watch to be executed at least once");
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);

        // we now know the watch is executing... lets deactivate it
        ActivateWatchResponse activateWatchResponse = new ActivateWatchRequestBuilder(client(), "_id", false).get();
        assertThat(activateWatchResponse, notNullValue());
        assertThat(activateWatchResponse.getStatus().state().isActive(), is(false));

        getWatchResponse = new GetWatchRequestBuilder(client(), "_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(false));

        // wait until no watch is executing
        assertBusy(() -> {
            WatcherStatsResponse statsResponse = new WatcherStatsRequestBuilder(client()).setIncludeCurrentWatches(true).get();
            int sum = statsResponse.getNodes().stream().map(WatcherStatsResponse.Node::getSnapshots).mapToInt(List::size).sum();
            assertThat(sum, is(0));
        });

        refresh();
        long count1 = docCount(".watcher-history*", matchAllQuery());

        // lets activate it again
        logger.info("Activating watch again");

        activateWatchResponse = new ActivateWatchRequestBuilder(client(), "_id", true).get();
        assertThat(activateWatchResponse, notNullValue());
        assertThat(activateWatchResponse.getStatus().state().isActive(), is(true));

        getWatchResponse = new GetWatchRequestBuilder(client(), "_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));

        refresh();
        assertBusy(() -> {
            long count2 = docCount(".watcher-history*", matchAllQuery());
            assertThat(count2, greaterThan(count1));
        });
    }

    public void testLoadWatchWithoutAState() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client())
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 1 ? 2050"))) // some time in 2050
                        .input(simpleInput("foo", "bar"))
                        .addAction("_a1", indexAction("actions"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client(), "_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));

        GetResponse getResponse = client().prepareGet().setIndex(".watches").setId("_id").get();
        XContentSource source = new XContentSource(getResponse.getSourceAsBytesRef(), XContentType.JSON);

        Set<String> filters = Sets.newHashSet(
                "trigger.**",
                "input.**",
                "condition.**",
                "throttle_period.**",
                "transform.**",
                "actions.**",
                "metadata.**",
                "status.version",
                "status.last_checked",
                "status.last_met_condition",
                "status.actions.**");

        XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), new BytesStreamOutput(), filters);
        source.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // now that we filtered out the watch status state, lets put it back in
        IndexResponse indexResponse = client().prepareIndex().setIndex(".watches").setId("_id")
                .setSource(BytesReference.bytes(builder), XContentType.JSON)
                .get();
        assertThat(indexResponse.getId(), is("_id"));

        // now, let's restart
        stopWatcher();
        startWatcher();

        getWatchResponse = new GetWatchRequestBuilder(client(), "_id").get();
        assertThat(getWatchResponse, notNullValue());
        assertThat(getWatchResponse.getStatus().state(), notNullValue());
        assertThat(getWatchResponse.getStatus().state().isActive(), is(true));
    }

}
