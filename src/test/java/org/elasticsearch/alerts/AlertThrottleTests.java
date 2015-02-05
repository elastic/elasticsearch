/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.history.HistoryService;
import org.elasticsearch.alerts.actions.IndexAlertAction;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class AlertThrottleTests extends AbstractAlertingTests {

    @Test
    public void testAckThrottle() throws Exception{
        AlertsClient alertsClient = alertClient();
        createIndex("action-index", "test-index");
        ensureGreen("action-index", "test-index");

        IndexResponse dummyEventIndexResponse = client().prepareIndex("test-index", "test-type").setSource( XContentFactory.jsonBuilder().startObject().field("test_field", "error").endObject()).get();
        assertTrue(dummyEventIndexResponse.isCreated());
        refresh();

        Alert alert = new Alert();
        alert.setStatus(Alert.Status.State.NOT_TRIGGERED);

        alert.setTriggerSearchRequest(createTriggerSearchRequest("test-index").source(searchSource().query(matchAllQuery())));
        alert.setTrigger(new ScriptTrigger("hits.total > 0", ScriptService.ScriptType.INLINE, "groovy"));
        alert.getActions().add(new IndexAlertAction("action-index", "action-type"));
        alert.setSchedule("0/5 * * * * ? *");
        alert.lastExecuteTime(new DateTime());
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert().setAlertName("throttled-alert").setAlertSource(jsonBuilder.bytes()).get();
        assertTrue(putAlertResponse.indexResponse().isCreated());

        Thread.sleep(20000);
        AckAlertResponse ackResponse = alertsClient.prepareAckAlert("throttled-alert").get();
        assertEquals(Alert.Status.ACKED, ackResponse.getStatus());

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

        //Now delete the event and the ack state should change to NOT_TRIGGERED
        DeleteResponse response = client().prepareDelete("test-index", "test-type", dummyEventIndexResponse.getId()).get();
        assertTrue(response.isFound());

        Thread.sleep(20000);
        GetAlertResponse getAlertResponse = alertsClient.prepareGetAlert("throttled-alert").get();
        assertTrue(getAlertResponse.getResponse().isExists());

        final AlertsStore alertsStore =
                internalTestCluster().getInstance(AlertsStore.class, internalTestCluster().getMasterName());

        Alert parsedAlert = alertsStore.parseAlert(getAlertResponse.getResponse().getId(),
                getAlertResponse.getResponse().getSourceAsBytesRef());
        assertThat(parsedAlert.getStatus(), equalTo(Alert.Status.NOT_TRIGGERED));

        CountResponse countOfThrottledActions = client()
                .prepareCount(HistoryService.ALERT_HISTORY_INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.matchQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.THROTTLED.toString()))
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

        Alert alert = new Alert();
        alert.setStatus(Alert.Status.NOT_ACKABLE);
        alert.setTriggerSearchRequest(createTriggerSearchRequest("test-index").source(searchSource().query(matchAllQuery())));
        alert.setTrigger(new ScriptTrigger("hits.total > 0", ScriptService.ScriptType.INLINE, "groovy"));
        alert.getActions().add(new IndexAlertAction("action-index", "action-type"));
        alert.setSchedule("0/5 * * * * ? *");

        alert.lastExecuteTime(new DateTime());
        alert.setThrottlePeriod(new TimeValue(10, TimeUnit.SECONDS));

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert().setAlertName("throttled-alert").setAlertSource(jsonBuilder.bytes()).get();
        assertTrue(putAlertResponse.indexResponse().isCreated());

        Thread.sleep(5*1000);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                refresh();
                CountResponse countResponse = client()
                        .prepareCount("action-index")
                        .setTypes("action-type")
                        .setSource(searchSource().query(matchAllQuery()).buildAsBytes())
                        .get();

                if (countResponse.getCount() != 1){
                    SearchResponse actionResponse = client().prepareSearch(HistoryService.ALERT_HISTORY_INDEX_PREFIX + "*")
                            .setQuery(matchAllQuery())
                            .get();
                    for (SearchHit hit : actionResponse.getHits()) {
                        logger.info("Got action hit [{}]", hit.getSourceRef().toUtf8());
                    }
                }

                assertThat(countResponse.getCount(), greaterThanOrEqualTo(1L));
            }
        });

        Thread.sleep(15*1000);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                refresh();
                CountResponse countResponse = client()
                        .prepareCount("action-index")
                        .setTypes("action-type")
                        .setSource(searchSource().query(matchAllQuery()).buildAsBytes())
                        .get();
                assertThat(countResponse.getCount(), greaterThanOrEqualTo(2L));
            }
        });

        CountResponse countOfThrottledActions = client()
                .prepareCount(HistoryService.ALERT_HISTORY_INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.matchQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.THROTTLED.toString()))
                .get();
        assertThat(countOfThrottledActions.getCount(), greaterThan(0L));
    }


}
