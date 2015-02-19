/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.actions.index.IndexAction;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.condition.script.ScriptCondition;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.history.HistoryStore;
import org.elasticsearch.alerts.input.search.SearchInput;
import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;
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


        SearchRequest request = createConditionSearchRequest("test-index").source(searchSource().query(matchAllQuery()));

        List<Action> actions = new ArrayList<>();

        actions.add(new IndexAction(logger, ClientProxy.of(client()), "action-index", "action-type"));

        Alert alert = new Alert(
                "test-ack-throttle",
                new CronSchedule("0/5 * * * * ? *"),
                new SearchInput(logger, scriptService(), ClientProxy.of(client()),
                        request),
                new ScriptCondition(logger, scriptService(), "hits.total > 0", ScriptService.ScriptType.INLINE, "groovy"),
                new SearchTransform(logger, scriptService(), ClientProxy.of(client()), request), new Actions(actions), null, new Alert.Status(), new TimeValue(0)
        );


        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert().setAlertName("throttled-alert").setAlertSource(jsonBuilder.bytes()).get();
        assertTrue(putAlertResponse.indexResponse().isCreated());

        Thread.sleep(20000);
        AckAlertResponse ackResponse = alertsClient.prepareAckAlert("throttled-alert").get();
        assertEquals(Alert.Status.AckStatus.State.ACKED, ackResponse.getStatus().ackStatus().state());

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

        SearchRequest request = createConditionSearchRequest("test-index").source(searchSource().query(matchAllQuery()));

        List<Action> actions = new ArrayList<>();

        actions.add(new IndexAction(logger, ClientProxy.of(client()), "action-index", "action-type"));

        Alert alert = new Alert(
                "test-time-throttle",
                new CronSchedule("0/5 * * * * ? *"),
                new SearchInput(logger, scriptService(), ClientProxy.of(client()),  request),
                new ScriptCondition(logger, scriptService(), "hits.total > 0", ScriptService.ScriptType.INLINE, "groovy"),
                new SearchTransform(logger, scriptService(), ClientProxy.of(client()), request),
                new Actions(actions), null, new Alert.Status(), new TimeValue(10, TimeUnit.SECONDS)
        );



        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert().setAlertName("throttled-alert").setAlertSource(jsonBuilder.bytes()).get();
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
            } catch (InterruptedException ie){

            }
        }
    }

}
