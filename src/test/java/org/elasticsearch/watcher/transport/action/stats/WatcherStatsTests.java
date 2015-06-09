/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.action.stats;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.watcher.WatcherBuild;
import org.elasticsearch.watcher.WatcherState;
import org.elasticsearch.watcher.WatcherVersion;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.condition.ConditionBuilders;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;


/**
 */
@ClusterScope(scope = TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false)
public class WatcherStatsTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testStartedStats() throws Exception {
        WatcherStatsRequest watcherStatsRequest = watcherClient().prepareWatcherStats().request();
        WatcherStatsResponse response = watcherClient().watcherStats(watcherStatsRequest).actionGet();

        assertThat(response.getWatcherState(), is(WatcherState.STARTED));
        assertThat(response.getExecutionQueueSize(), is(0L));
        assertThat(response.getWatchesCount(), is(0L));
        assertThat(response.getWatchExecutionQueueMaxSize(), is(timeWarped() ? 1L : 0L));
        assertThat(response.getVersion(), is(WatcherVersion.CURRENT));
        assertThat(response.getBuild(), is(WatcherBuild.CURRENT));
    }

    @Test
    public void testWatchCountStats() throws Exception {
        WatcherClient watcherClient = watcherClient();

        WatcherStatsRequest watcherStatsRequest = watcherClient.prepareWatcherStats().request();
        WatcherStatsResponse response = watcherClient.watcherStats(watcherStatsRequest).actionGet();

        assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));

        SearchRequest searchRequest = WatcherTestUtils.newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("* * * * * ? *")))
                        .input(searchInput(searchRequest))
                        .condition(ConditionBuilders.scriptCondition("ctx.payload.hits.total == 1"))
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
        assertThat(response.getWatchExecutionQueueMaxSize(), greaterThan(0L));
    }
}
