/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionEntry;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionState;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.alerts.triggers.ScriptedTrigger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class BootStrapTest extends AbstractAlertingTests {

    @Test
    public void testBootStrapAlerts() throws Exception {
        ensureAlertingStarted();

        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0 0/5 * * * ? *", searchRequest, "hits.total == 1");
        client().prepareIndex(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, "my-first-alert")
                .setSource(alertSource)
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .get();

        client().admin().indices().prepareRefresh(AlertsStore.ALERT_INDEX).get();
        stopAlerting();
        startAlerting();

        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();
        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));
    }

    @Test
    public void testBootStrapHistory() throws Exception {
        ensureAlertingStarted();
        internalTestCluster().ensureAtLeastNumDataNodes(2);

        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();
        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));

        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        Alert alert = new Alert("my-first-alert",
                searchRequest,
                new ScriptedTrigger("hits.total == 1", ScriptService.ScriptType.INLINE, "groovy"),
                new ArrayList< AlertAction>(),
                "0 0/5 * * * ? *",
                new DateTime(),
                0,
                new TimeValue(0),
                AlertAckState.NOT_ACKABLE);

        DateTime scheduledFireTime = new DateTime();
        AlertActionEntry entry = new AlertActionEntry(alert, scheduledFireTime, scheduledFireTime, AlertActionState.SEARCH_NEEDED);
        String actionHistoryIndex = AlertActionManager.getAlertHistoryIndexNameForTime(scheduledFireTime);

        createIndex(actionHistoryIndex);
        ensureGreen(actionHistoryIndex);

        IndexResponse indexResponse = client().prepareIndex(actionHistoryIndex, AlertActionManager.ALERT_HISTORY_TYPE, entry.getId())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setSource(XContentFactory.jsonBuilder().value(entry))
                .get();
        assertTrue(indexResponse.isCreated());
        client().admin().indices().prepareRefresh(actionHistoryIndex).get();

        stopAlerting();
        startAlerting();

        response = alertClient().prepareAlertsStats().get();
        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));
        assertThat(response.getAlertActionManagerLargestQueueSize(), equalTo(1L));
    }

    @Test
    public void testBootStrapManyHistoryIndices() throws Exception {
        int numberOfAlertHistoryEntriesPerIndex = randomIntBetween(5,10);
        int numberOfAlertHistoryIndices = randomIntBetween(2,8);
        DateTime now = new DateTime();
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));

        for (int i=0; i<numberOfAlertHistoryIndices; ++i) {
            DateTime historyIndexDate = now.minus((new TimeValue(i, TimeUnit.DAYS)).getMillis());
            String actionHistoryIndex = AlertActionManager.getAlertHistoryIndexNameForTime(historyIndexDate);
            createIndex(actionHistoryIndex);
            ensureGreen(actionHistoryIndex);
            for (int j=0; j<numberOfAlertHistoryEntriesPerIndex; ++j){
                Alert alert = new Alert("entryTestAlert" + i + "-" + j,
                        searchRequest,
                        new ScriptedTrigger("hits.total == 1", ScriptService.ScriptType.INLINE, "groovy"),
                        new ArrayList< AlertAction>(),
                        "0 0/5 * * * ? *",
                        new DateTime(),
                        0,
                        true,
                        new TimeValue(0),
                        AlertAckState.NOT_ACKABLE);
                AlertActionEntry entry = new AlertActionEntry(alert, historyIndexDate, historyIndexDate, AlertActionState.SEARCH_NEEDED);
                IndexResponse indexResponse = client().prepareIndex(actionHistoryIndex, AlertActionManager.ALERT_HISTORY_TYPE, entry.getId())
                        .setConsistencyLevel(WriteConsistencyLevel.ALL)
                        .setSource(XContentFactory.jsonBuilder().value(entry))
                        .get();
                assertTrue(indexResponse.isCreated());
            }
            client().admin().indices().prepareRefresh(actionHistoryIndex).get();
        }

        stopAlerting();
        startAlerting();
        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();

        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));
        assertThat(response.getAlertActionManagerLargestQueueSize(),
                equalTo((long)(numberOfAlertHistoryEntriesPerIndex*numberOfAlertHistoryIndices)));

    }


}
