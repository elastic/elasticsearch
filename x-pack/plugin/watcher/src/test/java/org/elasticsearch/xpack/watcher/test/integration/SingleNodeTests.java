/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, numDataNodes = 0, supportsDedicatedMasters = false)
public class SingleNodeTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        return false;
    }

    // Tests the standard setup when starting watcher in a regular cluster:
    // the index does not exist, a watch gets added and scheduled.
    // The watch should be executed properly, despite the index being created
    // and the cluster state listener being reloaded.
    public void testThatLoadingWithNonExistingIndexWorks() throws Exception {
        internalCluster().startNode();
        ensureLicenseEnabled();
        ensureWatcherTemplatesAdded();

        assertFalse(client().admin().indices().prepareExists(Watch.INDEX).get().isExists());
        startWatcher();

        final String watchId = randomAlphaOfLength(20);
        // now we start with an empty set up, store a watch and expected it to be executed
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch(watchId)
            .setSource(watchBuilder()
                .trigger(schedule(interval(1, IntervalSchedule.Interval.Unit.SECONDS)))
                .input(searchInput(templateRequest(searchSource().size(0).query(matchAllQuery()), ".watcher-history*")))
                .condition(new CompareCondition("ctx.payload.hits.total.value", CompareCondition.Op.LTE, 1L))
                .addAction("_logger", loggingAction("logging of watch " + watchId)))
            .get();
        assertThat(putWatchResponse.isCreated(), is(true));

        assertBusy(() -> {
            refresh(".watcher-history*");
            SearchResponse searchResponse = client().prepareSearch(".watcher-history*").setSize(0).get();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));

            final ActivateWatchResponse activateWatchResponse = watcherClient().prepareActivateWatch(watchId, false).get();
            assertThat(activateWatchResponse.getStatus().state().isActive(), is(false));
        });
    }

}
