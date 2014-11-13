/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.index.IndexAlertResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0)
public class BasicAlertingTest extends AbstractAlertingTests {

    @Test
    public void testIndexAlert() throws Exception {
        AlertsClientInterface alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = new SearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertsClient.prepareIndexAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();
        assertAlertTriggered("my-first-alert", 1);
    }

    @Test
    public void testIndexAlert_registerAlertBeforeTargetIndex() throws Exception {
        AlertsClientInterface alertsClient = alertClient();
        SearchRequest searchRequest = new SearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertsClient.prepareIndexAlert("my-first-alert")
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
        AlertsClientInterface alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = new SearchRequest("my-index").source(searchSource().query(matchAllQuery()));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        IndexAlertResponse indexResponse = alertsClient.prepareIndexAlert("my-first-alert")
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
        AlertsClientInterface alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        BytesReference alertSource = jsonBuilder().startObject()
                .field("schedule", "0/5 * * * * ? *")
                .startObject("trigger").startObject("script").field("script", "return true").endObject().endObject()
                .field("enable", true)
                .field("malformed_field", "x")
                .endObject().bytes();
        try {
            alertsClient.prepareIndexAlert("my-first-alert")
                    .setAlertSource(alertSource)
                    .get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
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
    public void testTriggerSearch() throws Exception {
        assertAcked(prepareCreate("my-index").addMapping("my-type", "_timestamp", "enabled=true", "event_type", "type=string"));

        SearchSourceBuilder searchSource = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{SCHEDULED_FIRE_TIME}}||-30s").to("{{SCHEDULED_FIRE_TIME}}"))
        );
        client().preparePutIndexedScript()
                .setScriptLang("mustache")
                .setId("my-template")
                .setSource(jsonBuilder().startObject().field("template").value(searchSource).endObject())
                .get();

        String alertName = "red-alert";
        long scheduleTimeInMs = 5000;
        SearchRequest[] searchRequests = new SearchRequest[]{
                new SearchRequest("my-index").source(searchSource)
//                client().prepareSearch("my-index").setTemplateName("my-template").request()
                // TODO: add template source based search requests
        };

        for (SearchRequest request : searchRequests) {
            logger.info("Running: {}", request);
            // A clean start. no data to trigger on and alert actions
            cluster().wipeIndices(AlertActionManager.ALERT_HISTORY_INDEX, "my-index");

            alertClient().prepareDeleteAlert(alertName).get();
            alertClient().prepareIndexAlert(alertName)
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
}
