/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test.integration;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.actions.ActionBuilders;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.history.HistoryStore;
import org.elasticsearch.alerts.test.AbstractAlertsIntegrationTests;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.alerts.client.AlertSourceBuilder.alertSourceBuilder;
import static org.elasticsearch.alerts.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.alerts.input.InputBuilders.searchInput;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.cron;
import static org.elasticsearch.alerts.scheduler.schedule.Schedules.interval;
import static org.elasticsearch.alerts.test.AlertsTestUtils.matchAllRequest;
import static org.elasticsearch.alerts.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class AlertThrottleTests extends AbstractAlertsIntegrationTests {

    @Test
    public void testAckThrottle() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("actions", "events");
        ensureGreen("actions", "events");

        IndexResponse eventIndexResponse = client().prepareIndex("events", "event")
                .setSource("level", "error")
                .get();
        assertThat(eventIndexResponse.isCreated(), is(true));
        refresh();

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert()
                .alertName("_name")
                .source(alertSourceBuilder()
                        .schedule(cron("0/5 * * * * ? *"))
                        .input(searchInput(matchAllRequest().indices("events")))
                        .condition(scriptCondition("ctx.payload.hits.total > 0"))
                        .transform(searchTransform(matchAllRequest().indices("events")))
                        .addAction(ActionBuilders.indexAction("actions", "action")))
                .get();

        assertThat(putAlertResponse.indexResponse().isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        AckAlertResponse ackResponse = alertsClient.prepareAckAlert("_name").get();
        assertThat(ackResponse.getStatus().ackStatus().state(), is(Alert.Status.AckStatus.State.ACKED));

        refresh();
        long countAfterAck = docCount("actions", "action", matchAllQuery());
        assertThat(countAfterAck, greaterThanOrEqualTo((long) 1));

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        refresh();

        // There shouldn't be more actions in the index after we ack the alert, even though the alert was fired
        long countAfterPostAckFires = docCount("actions", "action", matchAllQuery());
        assertThat(countAfterPostAckFires, equalTo(countAfterAck));

        //Now delete the event and the ack state should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete("events", "event", eventIndexResponse.getId()).get();
        assertThat(response.isFound(), is(true));
        refresh();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }

        GetAlertResponse getAlertResponse = alertsClient.prepareGetAlert("_name").get();
        assertThat(getAlertResponse.getResponse().isExists(), is(true));

        Alert parsedAlert = alertParser().parse(getAlertResponse.getResponse().getId(), true,
                getAlertResponse.getResponse().getSourceAsBytesRef());
        assertThat(parsedAlert.status().ackStatus().state(), is(Alert.Status.AckStatus.State.AWAITS_EXECUTION));

        long throttledCount = docCount(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*", null,
                matchQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.THROTTLED.id()));
        assertThat(throttledCount, greaterThan(0L));
    }


    @Test
    public void testTimeThrottle() throws Exception {
        AlertsClient alertsClient = alertClient();
        createIndex("actions", "events");
        ensureGreen("actions", "events");

        IndexResponse eventIndexResponse = client().prepareIndex("events", "event")
                .setSource("level", "error")
                .get();
        assertTrue(eventIndexResponse.isCreated());
        refresh();

        PutAlertResponse putAlertResponse = alertsClient.preparePutAlert()
                .alertName("_name")
                .source(alertSourceBuilder()
                        .schedule(interval("5s"))
                        .input(searchInput(matchAllRequest().indices("events")))
                        .condition(scriptCondition("ctx.payload.hits.total > 0"))
                        .transform(searchTransform(matchAllRequest().indices("events")))
                        .addAction(ActionBuilders.indexAction("actions", "action"))
                        .throttlePeriod(TimeValue.timeValueSeconds(10)))
                .get();
        assertThat(putAlertResponse.indexResponse().isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().clock().setTime(DateTime.now());

            timeWarp().scheduler().fire("_name");
            refresh();

            // the first fire should work
            long actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(1L));

            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().fire("_name");
            refresh();

            // the last fire should have been throttled, so number of actions shouldn't change
            actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(1L));

            timeWarp().clock().fastForwardSeconds(10);
            timeWarp().scheduler().fire("_name");
            refresh();

            // the last fire occurred passed the throttle period, so a new action should have been added
            actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(2L));

            long throttledCount = docCount(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*", null,
                    matchQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.THROTTLED.id()));
            assertThat(throttledCount, is(1L));

        } else {

            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            refresh();

            // the first fire should work so we should have a single action in the actions index
            long actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(1L));

            Thread.sleep(TimeUnit.SECONDS.toMillis(5));

            // we should still be within the throttling period... so the number of actions shouldn't change
            actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(1L));

            long throttledCount = docCount(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*", null,
                    matchQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.THROTTLED.id()));
            assertThat(throttledCount, greaterThanOrEqualTo(1L));
        }
    }

}
