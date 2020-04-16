/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Locale;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.noneInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThan;

public class WatcherConcreteIndexTests extends AbstractWatcherIntegrationTestCase {

    public void testCanUseAnyConcreteIndexName() throws Exception {
        String newWatcherIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String watchResultsIndex = randomAlphaOfLength(11).toLowerCase(Locale.ROOT);
        createIndex(watchResultsIndex);

        stopWatcher();
        replaceWatcherIndexWithRandomlyNamedIndex(Watch.INDEX, newWatcherIndexName);
        ensureGreen(newWatcherIndexName);
        startWatcher();

        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "mywatch").setSource(watchBuilder()
            .trigger(schedule(interval("3s")))
            .input(noneInput())
            .condition(InternalAlwaysCondition.INSTANCE)
            .addAction("indexer", indexAction(watchResultsIndex)))
            .get();

        assertTrue(putWatchResponse.isCreated());
        refresh();

        assertBusy(() -> timeWarp().trigger("mywatch"));

        assertBusy(() -> {
            SearchResponse searchResult = client().prepareSearch(watchResultsIndex).setTrackTotalHits(true).get();
            assertThat((int) searchResult.getHits().getTotalHits().value, greaterThan(0));
        });
    }
}
