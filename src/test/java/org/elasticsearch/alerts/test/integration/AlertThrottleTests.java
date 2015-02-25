/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test.integration;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.actions.ActionBuilders;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.history.HistoryStore;
import org.elasticsearch.alerts.test.AbstractAlertsIntegrationTests;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.alerts.client.AlertSourceBuilder.alertSourceBuilder;
import static org.elasticsearch.alerts.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.alerts.input.InputBuilders.searchInput;
import static org.elasticsearch.alerts.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.cron;
import static org.elasticsearch.alerts.test.AlertsTestUtils.matchAllRequest;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class AlertThrottleTests extends AbstractAlertsIntegrationTests {

    @Test
    public void testAckThrottle() throws Exception{
        AlertsClient alertsClient = alertClient();
        createIndex("action-index", "test-index");
        ensureGreen("action-index", "test-index");

        IndexResponse dummyEventIndexResponse = client().prepareIndex("test-index", "test-type").setSource( XContentFactory.jsonBuilder().startObject().field("test_field", "error").endObject()).get();
        assertTrue(dummyEventIndexResponse.isCreated());
        refresh();

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert()
                .alertName("throttled-alert")
                .source(alertSourceBuilder()
                        .schedule(cron("0/5 * * * * ? *"))
                        .input(searchInput(matchAllRequest().indices("test-index")))
                        .condition(scriptCondition("hits.total > 0"))
                        .transform(searchTransform(matchAllRequest().indices("test-index")))
                        .addAction(ActionBuilders.indexAction("action-index", "action-type"))
                        .throttlePeriod(TimeValue.timeValueMillis(0)))
                .get();

        assertTrue(putAlertResponse.indexResponse().isCreated());

        Thread.sleep(20000);
        AckAlertResponse ackResponse = alertsClient.prepareAckAlert("throttled-alert").get();
        Assert.assertEquals(Alert.Status.AckStatus.State.ACKED, ackResponse.getStatus().ackStatus().state());

        refresh();
        SearchResponse searchResponse = client()
                .prepareSearch("action-index")
                .setTypes("action-type")
                .setSearchType(SearchType.COUNT)
                .setSource(searchSource().query(matchAllQuery()).buildAsBytes())
                .get();
        long countAfterAck = searchResponse.getHits().getTotalHits();
        assertThat(countAfterAck, greaterThanOrEqualTo((long) 1));

        Thread.sleep(20000);
        refresh();
        searchResponse = client()
                .prepareSearch("action-index")
                .setTypes("action-type")
                .setSearchType(SearchType.COUNT)
                .setSource(searchSource().query(matchAllQuery()).buildAsBytes())
                .get();
        long countAfterSleep = searchResponse.getHits().getTotalHits();
        assertThat("There shouldn't be more entries in the index after we ack the alert", countAfterAck, equalTo(countAfterSleep));

        //Now delete the event and the ack state should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete("test-index", "test-type", dummyEventIndexResponse.getId()).get();
        assertTrue(response.isFound());

        Thread.sleep(20000);
        GetAlertResponse getAlertResponse = alertsClient.prepareGetAlert("throttled-alert").get();
        assertTrue(getAlertResponse.getResponse().isExists());

        final Alert.Parser alertParser  =
                internalTestCluster().getInstance(Alert.Parser.class, internalTestCluster().getMasterName());

        Alert parsedAlert = alertParser.parse(getAlertResponse.getResponse().getId(), true,
                getAlertResponse.getResponse().getSourceAsBytesRef());
        assertThat(parsedAlert.status().ackStatus().state(), equalTo(Alert.Status.AckStatus.State.AWAITS_EXECUTION));

        CountResponse countOfThrottledActions = client()
                .prepareCount(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.matchQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.THROTTLED.id()))
                .get();
        assertThat(countOfThrottledActions.getCount(), greaterThan(0L));
    }


    @Test
    public void testTimeThrottle() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("action-index", "test-index");
        ensureGreen("action-index", "test-index");

        IndexResponse dummyEventIndexResponse = client().prepareIndex("test-index", "test-type").setSource(XContentFactory.jsonBuilder().startObject().field("test_field", "error").endObject()).get();
        assertTrue(dummyEventIndexResponse.isCreated());
        refresh();

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert()
                .alertName("throttled-alert")
                .source(alertSourceBuilder()
                        .schedule(cron("0/5 * * * * ? *"))
                        .input(searchInput(matchAllRequest().indices("test-index")))
                        .condition(scriptCondition("hits.total > 0"))
                        .transform(searchTransform(matchAllRequest().indices("test-index")))
                        .addAction(ActionBuilders.indexAction("action-index", "action-type"))
                        .throttlePeriod(TimeValue.timeValueSeconds(10)))
                .get();
        assertTrue(putAlertResponse.indexResponse().isCreated());

        forceFullSleepTime(new TimeValue(5, TimeUnit.SECONDS));
        refresh();
        CountResponse countResponse = client()
                .prepareCount("action-index")
                .setTypes("action-type")
                .setSource(searchSource().query(matchAllQuery()).buildAsBytes())
                .get();

        if (countResponse.getCount() != 1){
            SearchResponse actionResponse = client().prepareSearch(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*")
                    .setQuery(matchAllQuery())
                    .get();
            for (SearchHit hit : actionResponse.getHits()) {
                logger.info("Got action hit [{}]", hit.getSourceRef().toUtf8());
            }
        }

        assertThat(countResponse.getCount(), greaterThanOrEqualTo(1L));
        assertThat(countResponse.getCount(), lessThanOrEqualTo(3L));

        forceFullSleepTime(new TimeValue(20, TimeUnit.SECONDS));

        refresh();
        countResponse = client()
                .prepareCount("action-index")
                .setTypes("action-type")
                .setSource(searchSource().query(matchAllQuery()).buildAsBytes())
                .get();
        assertThat(countResponse.getCount(), greaterThanOrEqualTo(2L));
        assertThat(countResponse.getCount(), lessThanOrEqualTo(4L));


        CountResponse countOfThrottledActions = client()
                .prepareCount(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.matchQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.THROTTLED.id()))
                .get();
        assertThat(countOfThrottledActions.getCount(), greaterThan(0L));
    }

    private void forceFullSleepTime(TimeValue value){
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() < start + value.getMillis()){
            try{
                Thread.sleep(value.getMillis() - (System.currentTimeMillis() - start));
            } catch (InterruptedException ie) {
                logger.error("interrupted", ie);
            }
        }
    }

}
