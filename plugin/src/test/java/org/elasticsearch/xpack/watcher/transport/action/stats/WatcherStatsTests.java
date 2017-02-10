/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.stats;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.watcher.WatcherState;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsResponse;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

@ClusterScope(scope = TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false)
@TestLogging("org.elasticsearch.watcher:TRACE")
public class WatcherStatsTests extends AbstractWatcherIntegrationTestCase {
    public void testStartedStats() throws Exception {
        WatcherStatsRequest watcherStatsRequest = watcherClient().prepareWatcherStats().request();
        WatcherStatsResponse response = watcherClient().watcherStats(watcherStatsRequest).actionGet();

        assertThat(response.getWatcherState(), is(WatcherState.STARTED));
        assertThat(response.getThreadPoolQueueSize(), is(0L));
        assertThat(response.getWatchesCount(), is(0L));
        assertThat(response.getThreadPoolMaxSize(), is(timeWarped() ? 1L : 0L));
    }

    public void testWatchCountStats() throws Exception {
        WatcherClient watcherClient = watcherClient();

        WatcherStatsRequest watcherStatsRequest = watcherClient.prepareWatcherStats().request();
        WatcherStatsResponse response = watcherClient.watcherStats(watcherStatsRequest).actionGet();

        assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));

        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "idx");
        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("* * * * * ? *")))
                        .input(searchInput(request))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                )
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name", 30, TimeValue.timeValueSeconds(1));
        } else {
            //Wait a little until we should have queued an action
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        }

        response = watcherClient().watcherStats(watcherStatsRequest).actionGet();

        assertThat(response.getWatcherState(), is(WatcherState.STARTED));
        assertThat(response.getWatchesCount(), is(1L));
        assertThat(response.getThreadPoolMaxSize(), greaterThan(0L));

        DeleteWatchResponse deleteWatchResponse = watcherClient.prepareDeleteWatch("_name").get();
        assertThat(deleteWatchResponse.isFound(), is(true));
    }
}
