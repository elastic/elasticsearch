/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;

public class ActionErrorIntegrationTests extends AbstractWatcherIntegrationTestCase {

    /**
     * This test makes sure that when an action encounters an error it should
     * not be subject to throttling. Also, the ack status of the action in the
     * watch should remain awaits_successful_execution as long as the execution
     * fails.
     */
    public void testErrorInAction() throws Exception {
        createIndex("foo");
        client().admin().indices().prepareUpdateSettings("foo").setSettings(Settings.builder().put("index.blocks.write", true)).get();

        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id").setSource(watchBuilder()
                .trigger(schedule(interval("10m")))

                        // adding an action that throws an error and is associated with a 60 minute throttle period
                        // with such a period, on successful execution we other executions of the watch will be
                        // throttled within the hour... but on failed execution there should be no throttling
                .addAction("_action", TimeValue.timeValueMinutes(60), IndexAction.builder("foo", "bar")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().trigger("_id");

        flush();

        // there should be a single history record with a failure status for the action:
        assertBusy(() -> {
            long count = watchRecordCount(QueryBuilders.boolQuery()
                    .must(termsQuery("result.actions.id", "_action"))
                    .must(termsQuery("result.actions.status", "failure")));
            assertThat(count, is(1L));
        });

        // now we'll trigger the watch again and make sure that it's not throttled and instead
        // writes another record to the history

        // within the 60 minute throttling period
        timeWarp().clock().fastForward(TimeValue.timeValueMinutes(randomIntBetween(1, 50)));
        timeWarp().trigger("_id");

        flush();

        // there should be a single history record with a failure status for the action:
        assertBusy(() -> {
            long count = watchRecordCount(QueryBuilders.boolQuery()
                    .must(termsQuery("result.actions.id", "_action"))
                    .must(termsQuery("result.actions.status", "failure")));
            assertThat(count, is(2L));
        });

        // now lets confirm that the ack status of the action is awaits_successful_execution
        GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client(), "_id").get();
        XContentSource watch = getWatchResponse.getSource();
        watch.getValue("status.actions._action.ack.awaits_successful_execution");
    }
}
