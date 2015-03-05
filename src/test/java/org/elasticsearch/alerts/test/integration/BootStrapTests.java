/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test.integration;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.condition.script.ScriptCondition;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.history.HistoryStore;
import org.elasticsearch.alerts.input.search.SearchInput;
import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.support.Script;
import org.elasticsearch.alerts.support.clock.SystemClock;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.test.AbstractAlertsIntegrationTests;
import org.elasticsearch.alerts.test.AlertsTestUtils;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class BootStrapTests extends AbstractAlertsIntegrationTests {

    @Test
    public void testBootStrapAlerts() throws Exception {
        ensureAlertingStarted();

        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0 0/5 * * * ? *", searchRequest, "ctx.payload.hits.total == 1");
        client().prepareIndex(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, "my-first-alert")
                .setSource(alertSource)
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .get();

        client().admin().indices().prepareRefresh(AlertsStore.ALERT_INDEX).get();
        stopAlerting();
        startAlerting();

        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();
        assertThat(response.isAlertActionManagerStarted(), is(true));
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));
    }

    @Test
    @TestLogging("alerts.actions:DEBUG")
    public void testBootstrapHistory() throws Exception {
        ensureAlertingStarted();

        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();
        assertThat(response.isAlertActionManagerStarted(), is(true));
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));

        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        Alert alert = new Alert(
                "test-serialization",
                SystemClock.INSTANCE,
                new CronSchedule("0/5 * * * * ? 2035"), //Set this into the future so we don't get any extra runs
                new SearchInput(logger, scriptService(), ClientProxy.of(client()), searchRequest),
                new ScriptCondition(logger, scriptService(), new Script("return true")),
                new SearchTransform(logger, scriptService(), ClientProxy.of(client()), searchRequest),
                new Actions(new ArrayList<Action>()),
                null, // metadata
                new TimeValue(0),
                new Alert.Status());

        XContentBuilder builder = jsonBuilder().value(alert);
        IndexResponse indexResponse = client().prepareIndex(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, alert.name())
                .setSource(builder).get();
        ensureGreen(AlertsStore.ALERT_INDEX);
        refresh();
        assertThat(indexResponse.isCreated(), is(true));

        DateTime scheduledFireTime = new DateTime(DateTimeZone.UTC);
        FiredAlert firedAlert = new FiredAlert(alert, scheduledFireTime, scheduledFireTime);
        String actionHistoryIndex = HistoryStore.getAlertHistoryIndexNameForTime(scheduledFireTime);

        createIndex(actionHistoryIndex);
        ensureGreen(actionHistoryIndex);
        logger.info("Created index {}", actionHistoryIndex);

        indexResponse = client().prepareIndex(actionHistoryIndex, HistoryStore.ALERT_HISTORY_TYPE, firedAlert.id())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setSource(jsonBuilder().value(firedAlert))
                .get();
        assertThat(indexResponse.isCreated(), is(true));

        stopAlerting();
        startAlerting();

        response = alertClient().prepareAlertsStats().get();
        assertThat(response.isAlertActionManagerStarted(), is(true));
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));
        assertThat(response.getAlertActionManagerLargestQueueSize(), greaterThanOrEqualTo(1l));
    }

    @Test
    @TestLogging("alerts.actions:DEBUG")
    public void testBootStrapManyHistoryIndices() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        long numberOfAlertHistoryIndices = randomIntBetween(2, 8);
        long numberOfAlertHistoryEntriesPerIndex = randomIntBetween(5, 10);
        SearchRequest searchRequest = AlertsTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));

        for (int i = 0; i < numberOfAlertHistoryIndices; i++) {
            DateTime historyIndexDate = now.minus((new TimeValue(i, TimeUnit.DAYS)).getMillis());
            String actionHistoryIndex = HistoryStore.getAlertHistoryIndexNameForTime(historyIndexDate);
            createIndex(actionHistoryIndex);
            ensureGreen(actionHistoryIndex);
            logger.info("Created index {}", actionHistoryIndex);

            for (int j = 0; j < numberOfAlertHistoryEntriesPerIndex; j++) {

                Alert alert = new Alert(
                        "action-test-" + i + " " + j,
                        SystemClock.INSTANCE,
                        new CronSchedule("0/5 * * * * ? 2035"), //Set a cron schedule far into the future so this alert is never scheduled
                        new SearchInput(logger, scriptService(), ClientProxy.of(client()),
                                searchRequest),
                        new ScriptCondition(logger, scriptService(), new Script("return true")),
                        new SearchTransform(logger, scriptService(), ClientProxy.of(client()), searchRequest),
                        new Actions(new ArrayList<Action>()),
                        null, // metatdata
                        new TimeValue(0),
                        new Alert.Status());
                XContentBuilder jsonBuilder = jsonBuilder();
                alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

                PutAlertResponse putAlertResponse = alertClient().preparePutAlert(alert.name()).source(jsonBuilder.bytes()).get();
                assertThat(putAlertResponse.indexResponse().isCreated(), is(true));

                FiredAlert firedAlert = new FiredAlert(alert, historyIndexDate, historyIndexDate);

                XContentBuilder jsonBuilder2 = jsonBuilder();
                firedAlert.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

                IndexResponse indexResponse = client().prepareIndex(actionHistoryIndex, HistoryStore.ALERT_HISTORY_TYPE, firedAlert.id())
                        .setConsistencyLevel(WriteConsistencyLevel.ALL)
                        .setSource(jsonBuilder2.bytes())
                        .get();
                assertThat(indexResponse.isCreated(), is(true));
            }
            client().admin().indices().prepareRefresh(actionHistoryIndex).get();
        }

        stopAlerting();
        startAlerting();
        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();

        assertThat(response.isAlertActionManagerStarted(), is(true));
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        final long totalHistoryEntries = numberOfAlertHistoryEntriesPerIndex * numberOfAlertHistoryIndices;

        assertBusy(new Runnable() {
            @Override
            public void run() {
                long count = docCount(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*", HistoryStore.ALERT_HISTORY_TYPE,
                        termQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.EXECUTED.id()));
                assertThat(count, is(totalHistoryEntries));
            }
        }, 30, TimeUnit.SECONDS);

    }


}
