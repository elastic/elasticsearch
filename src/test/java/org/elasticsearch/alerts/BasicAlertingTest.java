/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

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
public class BasicAlertingTest extends AbstractAlertingTests {

    @Test
    public void testIndexAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertsClient.preparePutAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();
        assertAlertTriggered("my-first-alert", 1);

        GetAlertResponse getAlertResponse = alertClient().prepareGetAlert().setAlertName("my-first-alert").get();
        assertThat(getAlertResponse.getResponse().isExists(), is(true));
        assertThat(getAlertResponse.getResponse().isSourceEmpty(), is(false));
    }

    @Test
    public void testIndexAlert_registerAlertBeforeTargetIndex() throws Exception {
        AlertsClient alertsClient = alertClient();
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertsClient.preparePutAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();

        // The alert can't trigger because there is no data that matches with the query
        assertNoAlertTrigger("my-first-alert", 1);

        // Index sample doc after we register the alert and the alert should get triggered
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        assertAlertTriggered("my-first-alert", 1);
    }

    @Test
    public void testDeleteAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(matchAllQuery()));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        PutAlertResponse indexResponse = alertsClient.preparePutAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();
        assertThat(indexResponse.indexResponse().isCreated(), is(true));

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

        alertSource.startObject("trigger").startObject("script").field("script", "return true").field("request");
        AlertUtils.writeSearchRequest(createTriggerSearchRequest(), alertSource, ToXContent.EMPTY_PARAMS);
        alertSource.endObject();

        alertSource.endObject();
        try {
            alertsClient.preparePutAlert("my-first-alert")
                    .setAlertSource(alertSource.bytes())
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
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(matchAllQuery()));
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
        // By accessing the actual hit we know that the fetch phase has been performed
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits?.hits[0]._score == 1.0");
        PutAlertResponse indexResponse = alertsClient.preparePutAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();
        assertThat(indexResponse.indexResponse().isCreated(), is(true));
        assertAlertTriggered("my-first-alert", 1);
    }

    @Test
    public void testModifyAlerts() throws Exception {
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(matchAllQuery()));
        alertClient().preparePutAlert("1")
                .setAlertSource(createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1"))
                .get();
        assertAlertTriggered("1", 0, false);

        alertClient().preparePutAlert("1")
                .setAlertSource(createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 0"))
                .get();
        assertAlertTriggered("1", 1, false);

        alertClient().preparePutAlert("1")
                .setAlertSource(createAlertSource("0/5 * * * * ? 2020", searchRequest, "hits.total == 0"))
                .get();

        Thread.sleep(5000);
        long triggered =  findNumberOfPerformedActions("1");
        Thread.sleep(5000);
        assertThat(triggered, equalTo(findNumberOfPerformedActions("1")));
    }

    @Test
    public void testAggregations() throws Exception {
        class R implements Runnable {

            private final long sleepTime;
            private final long totalTime;

            R(long sleepTime, long totalTime) {
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
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(
                searchSource()
                        .query(QueryBuilders.constantScoreQuery(FilterBuilders.rangeFilter("_timestamp").from("{{scheduled_fire_time}}||-1m").to("{{scheduled_fire_time}}")))
                        .aggregation(AggregationBuilders.dateHistogram("rate").field("_timestamp").interval(DateHistogram.Interval.SECOND).order(Histogram.Order.COUNT_DESC))
        );
        BytesReference reference = createAlertSource("* 0/1 * * * ? *", searchRequest, "aggregations.rate.buckets[0]?.doc_count > 5");
        alertClient().preparePutAlert("rate-alert").setAlertSource(reference).get();

        Thread indexThread = new Thread(new R(500, 60000));
        indexThread.start();
        indexThread.join();

        assertAlertTriggeredExact("rate-alert", 0);
        assertNoAlertTrigger("rate-alert", 1);

        indexThread = new Thread(new R(100, 60000));
        indexThread.start();
        indexThread.join();
        assertAlertTriggered("rate-alert", 1);
    }

    private final SearchSourceBuilder searchSourceBuilder = searchSource().query(
            filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{" + AlertUtils.SCHEDULED_FIRE_TIME_VARIABLE_NAME + "}}||-30s").to("{{" + AlertUtils.SCHEDULED_FIRE_TIME_VARIABLE_NAME + "}}"))
    );

    @Test
    public void testTriggerSearchWithSource() throws Exception {
        testTriggerSearch(
                createTriggerSearchRequest("my-index").source(searchSourceBuilder)
        );
    }

    @Test
    public void testTriggerSearchWithIndexedTemplate() throws Exception {
        client().preparePutIndexedScript()
                .setScriptLang("mustache")
                .setId("my-template")
                .setSource(jsonBuilder().startObject().field("template").value(searchSourceBuilder).endObject())
                .get();
        SearchRequest searchRequest = createTriggerSearchRequest("my-index");
        searchRequest.templateName("my-template");
        searchRequest.templateType(ScriptService.ScriptType.INDEXED);
        testTriggerSearch(searchRequest);
    }

    private void testTriggerSearch(SearchRequest request) throws Exception {
        long scheduleTimeInMs = 5000;
        String alertName = "red-alert";
        assertAcked(prepareCreate("my-index").addMapping("my-type", "_timestamp", "enabled=true", "event_type", "type=string"));

        alertClient().prepareDeleteAlert(alertName).get();
        alertClient().preparePutAlert(alertName)
                .setAlertSource(createAlertSource(String.format("0/%s * * * * ? *", (scheduleTimeInMs / 1000)), request, "return hits.total >= 3"))
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
        assertNoAlertTrigger(alertName, 1);

        time1 = System.currentTimeMillis();
        client().prepareIndex("my-index", "my-type")
                .setCreate(true)
                .setSource("event_type", "b")
                .get();
        timeLeft = scheduleTimeInMs - (System.currentTimeMillis() - time1);
        Thread.sleep(timeLeft);
        assertNoAlertTrigger(alertName, 2);

        time1 = System.currentTimeMillis();
        client().prepareIndex("my-index", "my-type")
                .setCreate(true)
                .setSource("event_type", "a")
                .get();
        timeLeft = scheduleTimeInMs - (System.currentTimeMillis() - time1);
        Thread.sleep(timeLeft);
        assertAlertTriggered(alertName, 1);
    }
}
