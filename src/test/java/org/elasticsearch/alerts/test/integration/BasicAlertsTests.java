/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.AlertsException;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.client.AlertSourceBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.scheduler.schedule.IntervalSchedule;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.test.AbstractAlertsIntegrationTests;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import static org.elasticsearch.alerts.actions.ActionBuilders.indexAction;
import static org.elasticsearch.alerts.client.AlertSourceBuilder.alertSourceBuilder;
import static org.elasticsearch.alerts.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.alerts.input.InputBuilders.searchInput;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.cron;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.interval;
import static org.elasticsearch.alerts.support.Variables.*;
import static org.elasticsearch.alerts.test.AlertsTestUtils.newInputSearchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 */
public class BasicAlertsTests extends AbstractAlertsIntegrationTests {

    @Test
    public void testIndexAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("idx");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        alertsClient.preparePutAlert("_name")
                .source(alertSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }

        assertAlertWithMinimumPerformedActionsCount("_name", 1);

        GetAlertResponse getAlertResponse = alertClient().prepareGetAlert().setAlertName("_name").get();
        assertThat(getAlertResponse.getResponse().isExists(), is(true));
        assertThat(getAlertResponse.getResponse().isSourceEmpty(), is(false));
    }

    @Test
    public void testIndexAlert_registerAlertBeforeTargetIndex() throws Exception {
        AlertsClient alertsClient = alertClient();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        alertsClient.preparePutAlert("_name")
                .source(alertSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }

        // The alert's condition won't meet because there is no data that matches with the query
        assertAlertWithNoActionNeeded("_name", 1);

        // Index sample doc after we register the alert and the alert's condition should meet
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }

        assertAlertWithMinimumPerformedActionsCount("_name", 1);
    }

    @Test
    public void testDeleteAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(matchAllQuery()));
        PutAlertResponse indexResponse = alertsClient.preparePutAlert("_name")
                .source(alertSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();
        assertThat(indexResponse.indexResponse().isCreated(), is(true));

        if (!timeWarped()) {
            // Although there is no added benefit in this test for waiting for the alert to fire, however
            // we need to wait here because of a test timing issue. When we tear down a test we delete the alert and delete all
            // indices, but there may still be inflight fired alerts, which may trigger the alert history to be created again, before
            // we finished the tear down phase.
            assertAlertWithNoActionNeeded("_name", 1);
        }

        DeleteAlertResponse deleteAlertResponse = alertsClient.prepareDeleteAlert("_name").get();
        assertThat(deleteAlertResponse.deleteResponse(), notNullValue());
        assertThat(deleteAlertResponse.deleteResponse().isFound(), is(true));

        refresh();
        assertHitCount(client().prepareCount(AlertsStore.ALERT_INDEX).get(), 0l);

        // Deleting the same alert for the second time
        deleteAlertResponse = alertsClient.prepareDeleteAlert("_name").get();
        assertThat(deleteAlertResponse.deleteResponse(), notNullValue());
        assertThat(deleteAlertResponse.deleteResponse().isFound(), is(false));
    }

    @Test
    public void testMalformedAlert() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("idx");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        XContentBuilder alertSource = jsonBuilder();

        alertSource.startObject();
        alertSource.field("unknown_field", "x");
        alertSource.startObject("schedule").field("cron", "0/5 * * * * ? *").endObject();

        alertSource.startObject("condition").startObject("script").field("script", "return true").field("request");
        AlertUtils.writeSearchRequest(newInputSearchRequest(), alertSource, ToXContent.EMPTY_PARAMS);
        alertSource.endObject();

        alertSource.endObject();
        try {
            alertsClient.preparePutAlert("_name")
                    .source(alertSource.bytes())
                    .get();
            fail();
        } catch (AlertsException e) {
            // In AlertStore we fail parsing if an alert contains undefined fields.
        }
        try {
            client().prepareIndex(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, "_name")
                    .setSource(alertSource)
                    .get();
            fail();
        } catch (Exception e) {
            // The alert index template the mapping is defined as strict
        }
    }

    @Test
    public void testModifyAlerts() throws Exception {
        SearchRequest searchRequest = newInputSearchRequest("idx")
                .source(searchSource().query(matchAllQuery()));

        AlertSourceBuilder source = alertSourceBuilder()
                .schedule(interval("5s"))
                .input(searchInput(searchRequest))
                .addAction(indexAction("idx", "action"));

        alertClient().preparePutAlert("_name")
                .source(source.condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }
        assertAlertWithMinimumPerformedActionsCount("_name", 0, false);

        alertClient().preparePutAlert("_name")
                .source(source.condition(scriptCondition("ctx.payload.hits.total == 0")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }
        assertAlertWithMinimumPerformedActionsCount("_name", 1, false);

        alertClient().preparePutAlert("_name")
                .source(source.schedule(cron("0/1 * * * * ? 2020")).condition(scriptCondition("ctx.payload.hits.total == 0")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        } else {
            Thread.sleep(1000);
        }

        long count =  findNumberOfPerformedActions("_name");

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        } else {
            Thread.sleep(1000);
        }

        assertThat(count, equalTo(findNumberOfPerformedActions("_name")));
    }

    @Test
    public void testConditionSearchWithSource() throws Exception {
        String variable = randomFrom(EXECUTION_TIME, SCHEDULED_FIRE_TIME, FIRE_TIME);
        SearchSourceBuilder searchSourceBuilder = searchSource().query(filteredQuery(
                matchQuery("level", "a"),
                rangeFilter("_timestamp")
                        .from("{{" + variable + "}}||-30s")
                        .to("{{" + variable + "}}")));

        testConditionSearch(newInputSearchRequest("events").source(searchSourceBuilder));
    }

    @Test
    public void testConditionSearchWithIndexedTemplate() throws Exception {
        String variable = randomFrom(EXECUTION_TIME, SCHEDULED_FIRE_TIME, FIRE_TIME);
        SearchSourceBuilder searchSourceBuilder = searchSource().query(filteredQuery(
                matchQuery("level", "a"),
                rangeFilter("_timestamp")
                        .from("{{" + variable + "}}||-30s")
                        .to("{{" + variable + "}}")));

        client().preparePutIndexedScript()
                .setScriptLang("mustache")
                .setId("my-template")
                .setSource(jsonBuilder().startObject().field("template").value(searchSourceBuilder).endObject())
                .get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("events");
        searchRequest.templateName("my-template");
        searchRequest.templateType(ScriptService.ScriptType.INDEXED);
        testConditionSearch(searchRequest);
    }

    private void testConditionSearch(SearchRequest request) throws Exception {
        String alertName = "_name";
        assertAcked(prepareCreate("events").addMapping("event", "_timestamp", "enabled=true", "level", "type=string"));

        alertClient().prepareDeleteAlert(alertName).get();
        alertClient().preparePutAlert(alertName)
                .source(createAlertSource(interval("5s"), request, "return ctx.payload.hits.total >= 3"))
                .get();

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        refresh();
        if (timeWarped()) {
            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().fire(alertName);
            refresh();
        } else {
            Thread.sleep(5000);
        }
        assertAlertWithNoActionNeeded(alertName, 1);

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "b")
                .get();
        refresh();
        if (timeWarped()) {
            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().fire(alertName);
            refresh();
        } else {
            Thread.sleep(5000);
        }
        assertAlertWithNoActionNeeded(alertName, 2);

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        refresh();
        if (timeWarped()) {
            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().fire(alertName);
            refresh();
        } else {
            Thread.sleep(5000);
        }
        assertAlertWithMinimumPerformedActionsCount(alertName, 1);
    }
}
