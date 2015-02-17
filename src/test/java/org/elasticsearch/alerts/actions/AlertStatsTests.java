/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.*;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsRequest;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.core.IsEqual.equalTo;


/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false)
public class AlertStatsTests extends AbstractAlertingTests {

    @Test
    public void testStartedStats() throws Exception {
        AlertsStatsRequest alertsStatsRequest = alertClient().prepareAlertsStats().request();
        AlertsStatsResponse response = alertClient().alertsStats(alertsStatsRequest).actionGet();

        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getAlertActionManagerQueueSize(), equalTo(0L));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));
        assertThat(response.getAlertActionManagerLargestQueueSize(), equalTo(0L));
        assertThat(response.getVersion(), equalTo(AlertsVersion.CURRENT));
        assertThat(response.getBuild(), equalTo(AlertsBuild.CURRENT));
    }

    @Test
    public void testAlertCountStats() throws Exception {
        AlertsClient alertsClient = alertClient();

        AlertsStatsRequest alertsStatsRequest = alertsClient.prepareAlertsStats().request();
        AlertsStatsResponse response = alertsClient.alertsStats(alertsStatsRequest).actionGet();

        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));

        SearchRequest searchRequest = createConditionSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("* * * * * ? *", searchRequest, "hits.total == 1");
        alertClient().preparePutAlert("testAlert")
                .setAlertSource(alertSource)
                .get();

        response = alertClient().alertsStats(alertsStatsRequest).actionGet();

        //Wait a little until we should have queued an action
        TimeValue waitTime = new TimeValue(30, TimeUnit.SECONDS);
        Thread.sleep(waitTime.getMillis());

        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));
        //assertThat(response.getAlertActionManagerLargestQueueSize(), greaterThan(0L));
    }
}
