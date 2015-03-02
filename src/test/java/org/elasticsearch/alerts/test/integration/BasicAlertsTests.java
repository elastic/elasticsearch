/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.AlertsException;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.client.AlertSourceBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.scheduler.schedule.IntervalSchedule;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.Variables;
import org.elasticsearch.alerts.test.AbstractAlertsIntegrationTests;
import org.elasticsearch.alerts.test.AlertsTestUtils;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.Locale;

import static org.elasticsearch.alerts.actions.ActionBuilders.indexAction;
import static org.elasticsearch.alerts.client.AlertSourceBuilder.alertSourceBuilder;
import static org.elasticsearch.alerts.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.alerts.input.InputBuilders.searchInput;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.cron;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.interval;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
public class BasicAlertsTests extends AbstractAlertsIntegrationTests {

    @Test
    public void testIndexAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        alertsClient.preparePutAlert("my-first-alert")
                .source(alertSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();
        assertAlertWithMinimumPerformedActionsCount("my-first-alert", 1);

        GetAlertResponse getAlertResponse = alertClient().prepareGetAlert().setAlertName("my-first-alert").get();
        assertThat(getAlertResponse.getResponse().isExists(), is(true));
        assertThat(getAlertResponse.getResponse().isSourceEmpty(), is(false));
    }

    @Test
    public void testIndexAlert_registerAlertBeforeTargetIndex() throws Exception {
        AlertsClient alertsClient = alertClient();
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        alertsClient.preparePutAlert("my-first-alert")
                .source(alertSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        // The alert's condition won't meet because there is no data that matches with the query
        assertAlertWithNoActionNeeded("my-first-alert", 1);

        // Index sample doc after we register the alert and the alert's condition should meet
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        assertAlertWithMinimumPerformedActionsCount("my-first-alert", 1);
    }

    @Test
    @TestLogging("action.admin.indices.delete:TRACE")
    public void testDeleteAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(searchSource().query(matchAllQuery()));
        PutAlertResponse indexResponse = alertsClient.preparePutAlert("my-first-alert")
                .source(alertSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();
        assertThat(indexResponse.indexResponse().isCreated(), is(true));

        // TODO: when MockScheduler can be used this workaround can be removed:
        // Although there is no added benefit in this test for waiting for the alert to fire, however
        // we need to wait here because of a test timing issue. When we tear down a test we delete the alert and delete all
        // indices, but there may still be inflight fired alerts, which may trigger the alert history to be created again, before
        // we finished the tear down phase.
        assertAlertWithNoActionNeeded("my-first-alert", 1);

        DeleteAlertRequest deleteAlertRequest = new DeleteAlertRequest("my-first-alert");
        DeleteAlertResponse deleteAlertResponse = alertsClient.deleteAlert(deleteAlertRequest).actionGet();
        assertNotNull(deleteAlertResponse.deleteResponse());
        assertTrue(deleteAlertResponse.deleteResponse().isFound());

        refresh();
        assertHitCount(client().prepareCount(AlertsStore.ALERT_INDEX).get(), 0l);

        // Deleting the same alert for the second time
        deleteAlertRequest = new DeleteAlertRequest("my-first-alert");
        deleteAlertResponse = alertsClient.deleteAlert(deleteAlertRequest).actionGet();
        assertNotNull(deleteAlertResponse.deleteResponse());
        assertFalse(deleteAlertResponse.deleteResponse().isFound());
    }

    @Test
    public void testMalformedAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        XContentBuilder alertSource = jsonBuilder();

        alertSource.startObject();
        alertSource.field("malformed_field", "x");
        alertSource.startObject("schedule").field("cron", "0/5 * * * * ? *").endObject();

        alertSource.startObject("condition").startObject("script").field("script", "return true").field("request");
        AlertUtils.writeSearchRequest(AlertsTestUtils.newInputSearchRequest(), alertSource, ToXContent.EMPTY_PARAMS);
        alertSource.endObject();

        alertSource.endObject();
        try {
            alertsClient.preparePutAlert("my-first-alert")
                    .source(alertSource.bytes())
                    .get();
            fail();
        } catch (AlertsException e) {
            // In AlertStore we fail parsing if an alert contains undefined fields.
        }
        try {
            client().prepareIndex(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, "my-first-alert")
                    .setSource(alertSource)
                    .get();
            fail();
        } catch (Exception e) {
            // The alert index template the mapping is defined as strict
        }
    }

    @Test
    public void testAlertWithDifferentSearchType() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(searchSource().query(matchAllQuery()));
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
        // By accessing the actual hit we know that the fetch phase has been performed
        PutAlertResponse indexResponse = alertsClient.preparePutAlert("my-first-alert")
                .source(alertSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits?.hits[0]._score == 1.0")))
                .get();
        assertThat(indexResponse.indexResponse().isCreated(), is(true));
        assertAlertWithMinimumPerformedActionsCount("my-first-alert", 1);
    }

    @Test
    public void testModifyAlerts() throws Exception {
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index")
                .source(searchSource().query(matchAllQuery()));

        AlertSourceBuilder source = alertSourceBuilder()
                .schedule(cron("0/5 * * * * ? *"))
                .input(searchInput(searchRequest))
                .addAction(indexAction("my-index", "trail"));


        alertClient().preparePutAlert("1")
                .source(source.condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();
        assertAlertWithMinimumPerformedActionsCount("1", 0, false);

        alertClient().preparePutAlert("1")
                .source(source.condition(scriptCondition("ctx.payload.hits.total == 0")))
                .get();
        assertAlertWithMinimumPerformedActionsCount("1", 1, false);

        alertClient().preparePutAlert("1")
                .source(source.schedule(cron("0/5 * * * * ? 2020")).condition(scriptCondition("ctx.payload.hits.total == 0")))
                .get();

        Thread.sleep(5000);
        long count =  findNumberOfPerformedActions("1");
        Thread.sleep(5000);
        assertThat(count, equalTo(findNumberOfPerformedActions("1")));
    }

    @Test
    public void testAggregations() throws Exception {

        class Indexer extends Thread {

            private final long sleepTime;
            private final long totalTime;

            Indexer(long sleepTime, long totalTime) {
                this.sleepTime = sleepTime;
                this.totalTime = totalTime;
            }

            @Override
            public void run() {
                long startTime = System.currentTimeMillis();
                while ((System.currentTimeMillis() - startTime) < totalTime) {
                    client().prepareIndex("my-index", "my-type").setCreate(true).setSource("{}").get();
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        assertAcked(prepareCreate("my-index").addMapping("my-type", "_timestamp", "enabled=true"));
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(
                searchSource()
                        .query(QueryBuilders.constantScoreQuery(FilterBuilders.rangeFilter("_timestamp").from("{{scheduled_fire_time}}||-1m").to("{{scheduled_fire_time}}")))
                        .aggregation(AggregationBuilders.dateHistogram("rate").field("_timestamp").interval(DateHistogram.Interval.SECOND).order(Histogram.Order.COUNT_DESC)));
//        BytesReference reference = createAlertSource("* 0/1 * * * ? *", searchRequest, "aggregations.rate.buckets[0]?.doc_count > 5");
        alertClient().preparePutAlert("rate-alert")
                .source(alertSourceBuilder()
                        .schedule(cron("* 0/1 * * * ? *"))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.aggregations.rate.buckets[0]?.doc_count > 5"))
                        .addAction(indexAction("my-index", "trail")))
                .get();

        Indexer indexer = new Indexer(500, 60000);
        indexer.start();
        indexer.join();

        assertAlertWithExactPerformedActionsCount("rate-alert", 0);
        assertAlertWithNoActionNeeded("rate-alert", 1);

        indexer = new Indexer(100, 60000);
        indexer.start();
        indexer.join();

        assertAlertWithMinimumPerformedActionsCount("rate-alert", 1);
    }

    private final SearchSourceBuilder searchSourceBuilder = searchSource().query(
            filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{" + Variables.SCHEDULED_FIRE_TIME + "}}||-30s").to("{{" + Variables.SCHEDULED_FIRE_TIME + "}}"))
    );

    @Test
    public void testConditionSearchWithSource() throws Exception {
        testConditionSearch(
                AlertsTestUtils.newInputSearchRequest("my-index").source(searchSourceBuilder)
        );
    }

    @Test
    public void testConditionSearchWithIndexedTemplate() throws Exception {
        client().preparePutIndexedScript()
                .setScriptLang("mustache")
                .setId("my-template")
                .setSource(jsonBuilder().startObject().field("template").value(searchSourceBuilder).endObject())
                .get();
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index");
        searchRequest.templateName("my-template");
        searchRequest.templateType(ScriptService.ScriptType.INDEXED);
        testConditionSearch(searchRequest);
    }

    private void testConditionSearch(SearchRequest request) throws Exception {
        long scheduleTimeInMs = 5000;
        String alertName = "red-alert";
        assertAcked(prepareCreate("my-index").addMapping("my-type", "_timestamp", "enabled=true", "event_type", "type=string"));

        alertClient().prepareDeleteAlert(alertName).get();
        alertClient().preparePutAlert(alertName)
                .source(createAlertSource(String.format(Locale.ROOT, "0/%s * * * * ? *", (scheduleTimeInMs / 1000)), request, "return ctx.payload.hits.total >= 3"))
                .get();

        long time1 = System.currentTimeMillis();
        client().prepareIndex("my-index", "my-type")
                .setCreate(true)
                .setSource("event_type", "a")
                .get();
        client().prepareIndex("my-index", "my-type")
                .setCreate(true)
                .setSource("event_type", "a")
                .get();
        long timeLeft = scheduleTimeInMs - (System.currentTimeMillis() - time1);
        Thread.sleep(timeLeft);
        assertAlertWithNoActionNeeded(alertName, 1);

        time1 = System.currentTimeMillis();
        client().prepareIndex("my-index", "my-type")
                .setCreate(true)
                .setSource("event_type", "b")
                .get();
        timeLeft = scheduleTimeInMs - (System.currentTimeMillis() - time1);
        Thread.sleep(timeLeft);
        assertAlertWithNoActionNeeded(alertName, 2);

        time1 = System.currentTimeMillis();
        client().prepareIndex("my-index", "my-type")
                .setCreate(true)
                .setSource("event_type", "a")
                .get();
        timeLeft = scheduleTimeInMs - (System.currentTimeMillis() - time1);
        Thread.sleep(timeLeft);
        assertAlertWithMinimumPerformedActionsCount(alertName, 1);
    }
}
