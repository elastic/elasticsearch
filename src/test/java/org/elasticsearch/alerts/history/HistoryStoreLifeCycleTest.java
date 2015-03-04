/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.alerts.support.clock.SystemClock;
import org.elasticsearch.alerts.test.AbstractAlertsSingleNodeTests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
public class HistoryStoreLifeCycleTest extends AbstractAlertsSingleNodeTests {

    @Test
    public void testPutLoadUpdate() throws Exception {
        Condition condition = new AlwaysTrueCondition(logger);
        HistoryStore historyStore = getInstanceFromNode(HistoryStore.class);
        Alert alert = new Alert("_name", SystemClock.INSTANCE, null, null, condition, null, null, null, null, null);

        // Put fired alerts and verify that these are stored
        FiredAlert[] firedAlerts = new FiredAlert[randomIntBetween(1, 50)];
        for (int i = 0; i < firedAlerts.length; i++) {
            DateTime dateTime = new DateTime(i, DateTimeZone.UTC);
            firedAlerts[i] = new FiredAlert(alert, dateTime, dateTime);
            historyStore.put(firedAlerts[i]);
            GetResponse getResponse = client().prepareGet(HistoryStore.getAlertHistoryIndexNameForTime(dateTime), HistoryStore.ALERT_HISTORY_TYPE, firedAlerts[i].id())
                    .setVersion(1)
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
        }

        // Load the stored alerts
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        HistoryStore.LoadResult loadResult = historyStore.loadFiredAlerts(clusterService.state(), FiredAlert.State.AWAITS_EXECUTION);
        assertThat(loadResult.succeeded(), is(true));
        List<FiredAlert> loadedFiredAlerts = ImmutableList.copyOf(loadResult);
        assertThat(loadedFiredAlerts.size(), equalTo(firedAlerts.length));

        // Change the state to executed and update the alerts and then verify if the changes have been persisted too
        for (FiredAlert firedAlert : firedAlerts) {
            assertThat(loadedFiredAlerts.contains(firedAlert), is(true));
            assertThat(firedAlert.version(), equalTo(1l));
            firedAlert.update(FiredAlert.State.EXECUTED, "_message");
            historyStore.update(firedAlert);
            GetResponse getResponse = client().prepareGet(HistoryStore.getAlertHistoryIndexNameForTime(firedAlert.scheduledTime()), HistoryStore.ALERT_HISTORY_TYPE, firedAlert.id())
                    .setVersion(2l)
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
        }

        // try to load fired alerts, but none are in the await state, so no alerts are loaded.
        loadResult = historyStore.loadFiredAlerts(clusterService.state(), FiredAlert.State.AWAITS_EXECUTION);
        assertThat(loadResult.succeeded(), is(true));
        loadedFiredAlerts = ImmutableList.copyOf(loadResult);
        assertThat(loadedFiredAlerts.size(), equalTo(0));
    }

}
