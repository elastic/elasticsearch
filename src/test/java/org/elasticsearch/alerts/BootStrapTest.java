/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionEntry;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionState;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsRequest;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.alerts.triggers.ScriptedTrigger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numClientNodes = 0, transportClientRatio = 0, numDataNodes = 3)
public class BootStrapTest extends AbstractAlertingTests {

    @Test
    public void testBootStrapAlerts() throws Exception {
        ensureGreen();

        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0 0/5 * * * ? *", searchRequest, "hits.total == 1");
        alertClient().prepareIndexAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();

        AlertsStatsRequest alertsStatsRequest = alertClient().prepareAlertsStats().request();
        AlertsStatsResponse response = alertClient().alertsStats(alertsStatsRequest).actionGet();

        assertTrue(response.isAlertActionManagerStarted());
        assertTrue(response.isAlertManagerStarted());
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));

        client().admin().indices().prepareRefresh(AlertsStore.ALERT_INDEX).get();


        String oldMaster = internalTestCluster().getMasterName();

        try {
            internalTestCluster().stopCurrentMasterNode();
        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to stop current master", ioe);
        }

        //Wait for alerts to start
        TimeValue maxTime = new TimeValue(30, TimeUnit.SECONDS);
        Thread.sleep(maxTime.getMillis());

        String newMaster = internalTestCluster().getMasterName();

        assertFalse(newMaster.equals(oldMaster));
        logger.info("Switched master from [{}] to [{}]",oldMaster,newMaster);

        alertsStatsRequest = alertClient().prepareAlertsStats().request();
        response = alertClient().alertsStats(alertsStatsRequest).actionGet();

        assertTrue(response.isAlertActionManagerStarted());
        assertTrue(response.isAlertManagerStarted());

        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));
    }

    @Test
    public void testBootStrapHistory() throws Exception {
        ensureGreen();

        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        AlertsStatsRequest alertsStatsRequest = alertClient().prepareAlertsStats().request();
        AlertsStatsResponse response = alertClient().alertsStats(alertsStatsRequest).actionGet();

        assertTrue(response.isAlertActionManagerStarted());
        assertTrue(response.isAlertManagerStarted());
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));

        Alert alert = new Alert("my-first-alert",
                searchRequest,
                new ScriptedTrigger("hits.total == 1", ScriptService.ScriptType.INLINE, "groovy"),
                new ArrayList< AlertAction>(),
                "0 0/5 * * * ? *",
                new DateTime(),
                0,
                true,
                new TimeValue(0),
                AlertAckState.NOT_ACKABLE);

        AlertActionEntry entry =
                new AlertActionEntry(alert,
                        new DateTime(),
                        new DateTime(),
                        AlertActionState.SEARCH_NEEDED);

        IndexResponse indexResponse = client().prepareIndex().setIndex(AlertActionManager.ALERT_HISTORY_INDEX).setType(AlertActionManager.ALERT_HISTORY_TYPE).setId(entry.getId())
                .setSource(XContentFactory.jsonBuilder().value(entry))
                .get();

        assertTrue(indexResponse.isCreated());

        GetResponse getResponse = client().prepareGet(AlertActionManager.ALERT_HISTORY_INDEX, AlertActionManager.ALERT_HISTORY_TYPE, entry.getId()).get();
        assertTrue(getResponse.isExists());
        assertEquals(getResponse.getId(), entry.getId());
        logger.info("Successfully indexed [{}]", entry.getId());

        Map<String,Object> responseMap = getResponse.getSourceAsMap();

        logger.info("State [{}]", responseMap.get(AlertActionState.FIELD_NAME) );

        //client().admin().indices().prepareRefresh(AlertActionManager.ALERT_HISTORY_INDEX).get();

        String oldMaster = internalTestCluster().getMasterName();

        try {
            internalTestCluster().stopCurrentMasterNode();
        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to stop current master", ioe);
        }

        //Wait for alerts to start
        TimeValue maxTime = new TimeValue(30, TimeUnit.SECONDS);
        Thread.sleep(maxTime.getMillis());

        String newMaster = internalTestCluster().getMasterName();

        assertFalse(newMaster.equals(oldMaster));
        logger.info("Switched master from [{}] to [{}]",oldMaster,newMaster);

        alertsStatsRequest = alertClient().prepareAlertsStats().request();
        response = alertClient().alertsStats(alertsStatsRequest).actionGet();

        assertTrue(response.isAlertActionManagerStarted());
        assertTrue(response.isAlertManagerStarted());

        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));
        assertThat(response.getAlertActionManagerLargestQueueSize(), equalTo(1L));

    }

}
