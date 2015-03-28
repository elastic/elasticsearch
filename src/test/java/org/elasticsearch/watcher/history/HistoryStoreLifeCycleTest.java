/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.Matchers.*;

/**
 */
public class HistoryStoreLifeCycleTest extends AbstractWatcherIntegrationTests {

    @Test
    public void testPutLoadUpdate() throws Exception {
        Condition condition = new AlwaysTrueCondition(logger);
        HistoryStore historyStore = getInstanceFromMaster(HistoryStore.class);
        Watch watch = new Watch("_name", SystemClock.INSTANCE, null, null, condition, null, null, null, null, null);

        // Put watch records and verify that these are stored
        WatchRecord[] watchRecords = new WatchRecord[randomIntBetween(1, 50)];
        for (int i = 0; i < watchRecords.length; i++) {
            DateTime dateTime = new DateTime(i, DateTimeZone.UTC);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(dateTime, dateTime);
            watchRecords[i] = new WatchRecord(watch, event);
            historyStore.put(watchRecords[i]);
            GetResponse getResponse = client().prepareGet(HistoryStore.getHistoryIndexNameForTime(dateTime), HistoryStore.DOC_TYPE, watchRecords[i].id())
                    .setVersion(1)
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
        }

        // Load the stored watch records
        ClusterService clusterService = getInstanceFromMaster(ClusterService.class);
        Collection<WatchRecord> records = historyStore.loadRecords(clusterService.state(), WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, notNullValue());
        assertThat(records, hasSize(watchRecords.length));

        // Change the state to executed and update the watch records and then verify if the changes have been persisted too
        for (WatchRecord watchRecord : watchRecords) {
            assertThat(records.contains(watchRecord), is(true));
            assertThat(watchRecord.version(), equalTo(1l));
            watchRecord.update(WatchRecord.State.EXECUTED, "_message");
            historyStore.update(watchRecord);
            GetResponse getResponse = client().prepareGet(HistoryStore.getHistoryIndexNameForTime(watchRecord.triggerEvent().triggeredTime()), HistoryStore.DOC_TYPE, watchRecord.id())
                    .setVersion(2l)
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
        }

        // try to load watch records, but none are in the await state, so no watch records are loaded.
        records = historyStore.loadRecords(clusterService.state(), WatchRecord.State.AWAITS_EXECUTION);
        assertThat(records, notNullValue());
        assertThat(records, hasSize(0));
    }

}
