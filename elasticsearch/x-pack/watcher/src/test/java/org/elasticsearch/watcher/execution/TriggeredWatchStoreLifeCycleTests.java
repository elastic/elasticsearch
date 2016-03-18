/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class TriggeredWatchStoreLifeCycleTests extends AbstractWatcherIntegrationTestCase {
    public void testPutLoadUpdate() throws Exception {
        ExecutableCondition condition = new ExecutableAlwaysCondition(logger);
        TriggeredWatchStore triggeredWatchStore = getInstanceFromMaster(TriggeredWatchStore.class);
        Watch watch = new Watch("_name", null, new ExecutableNoneInput(logger), condition, null, null, null, null, null);

        // Put watch records and verify that these are stored
        TriggeredWatch[] triggeredWatches = new TriggeredWatch[randomIntBetween(1, 50)];
        for (int i = 0; i < triggeredWatches.length; i++) {
            DateTime dateTime = new DateTime(i, DateTimeZone.UTC);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watch.id(), dateTime, dateTime);
            Wid wid = new Wid("record_" + i, randomLong(), DateTime.now(DateTimeZone.UTC));
            triggeredWatches[i] = new TriggeredWatch(wid, event);
            triggeredWatchStore.put(triggeredWatches[i]);
            GetResponse getResponse = client().prepareGet(TriggeredWatchStore.INDEX_NAME, TriggeredWatchStore.DOC_TYPE,
                    triggeredWatches[i].id().value())
                    .setVersion(1)
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
        }

        // Load the stored watch records
        ClusterService clusterService = getInstanceFromMaster(ClusterService.class);
        Collection<TriggeredWatch> loadedTriggeredWatches = triggeredWatchStore.loadTriggeredWatches(clusterService.state());
        assertThat(loadedTriggeredWatches, notNullValue());
        assertThat(loadedTriggeredWatches, hasSize(triggeredWatches.length));

        // Change the state to executed and update the watch records and then verify if the changes have been persisted too
        for (TriggeredWatch triggeredWatch : triggeredWatches) {
            assertThat(loadedTriggeredWatches.contains(triggeredWatch), is(true));
            triggeredWatchStore.delete(triggeredWatch.id());
            GetResponse getResponse = client().prepareGet(TriggeredWatchStore.INDEX_NAME, TriggeredWatchStore.DOC_TYPE,
                    triggeredWatch.id().value())
                    .setVersion(2L)
                    .get();
            assertThat(getResponse.isExists(), equalTo(false));
        }

        // try to load watch records, but none are in the await state, so no watch records are loaded.
        loadedTriggeredWatches = triggeredWatchStore.loadTriggeredWatches(clusterService.state());
        assertThat(loadedTriggeredWatches, notNullValue());
        assertThat(loadedTriggeredWatches, hasSize(0));
    }
}
