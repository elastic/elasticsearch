/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.stats;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

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

@ClusterScope(scope = TEST, numClientNodes = 0, transportClientRatio = 0)
public class WatcherStatsTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        return true;
    }

    public void testStartedStats() throws Exception {
        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();

        assertThat(response.getWatchesCount(), is(0L));
        response.getNodes().forEach(nodeResponse -> {
            assertThat(nodeResponse.getThreadPoolQueueSize(), is(0L));
            assertThat(nodeResponse.getThreadPoolMaxSize(), is(timeWarped() ? 1L : 0L));
        });

        boolean isWatcherStarted = response.getNodes().stream().allMatch(node -> node.getWatcherState() == WatcherState.STARTED);
        assertThat("expected watcher to be started on all nodes", isWatcherStarted, is(true));
    }

    public void testWatchCountStats() throws Exception {
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "idx");
        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("* * * * * ? *")))
                        .input(searchInput(request))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                )
                .get();

        timeWarp().trigger("_name", 30, TimeValue.timeValueSeconds(1));

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();

        assertThat(response.getWatchesCount(), is(1L));
        response.getNodes().forEach(nodeResponse -> assertThat(nodeResponse.getThreadPoolMaxSize(), greaterThan(0L)));

        DeleteWatchResponse deleteWatchResponse = watcherClient().prepareDeleteWatch("_name").get();
        assertThat(deleteWatchResponse.isFound(), is(true));
    }
}
