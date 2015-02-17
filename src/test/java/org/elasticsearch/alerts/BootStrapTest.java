/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.condition.search.ScriptSearchCondition;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.history.HistoryStore;
import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class BootStrapTest extends AbstractAlertingTests {

    @Test
    public void testBootStrapAlerts() throws Exception {
        ensureAlertingStarted();

        SearchRequest searchRequest = createConditionSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0 0/5 * * * ? *", searchRequest, "hits.total == 1");
        client().prepareIndex(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, "my-first-alert")
                .setSource(alertSource)
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .get();

        client().admin().indices().prepareRefresh(AlertsStore.ALERT_INDEX).get();
        stopAlerting();
        startAlerting();

        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();
        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));
    }

    @Test
    @TestLogging("alerts.actions:DEBUG")
    public void testBootstrapHistory() throws Exception {
        ensureAlertingStarted();

        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();
        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(0L));

        SearchRequest searchRequest = createConditionSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        Alert alert = new Alert(
                "test-serialization",
                new CronSchedule("0/5 * * * * ? 2035"),
                new ScriptSearchCondition(logger, ScriptServiceProxy.of(scriptService()), ClientProxy.of(client()),
                        searchRequest, "return true", ScriptService.ScriptType.INLINE, "groovy"),
                new SearchTransform(logger, ScriptServiceProxy.of(scriptService()), ClientProxy.of(client()), searchRequest),
                new TimeValue(0),
                new Actions(new ArrayList<Action>()),
                null,
                new Alert.Status()
        );

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
        assertTrue(indexResponse.isCreated());

        stopAlerting();
        startAlerting();

        response = alertClient().prepareAlertsStats().get();
        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        assertThat(response.getNumberOfRegisteredAlerts(), equalTo(1L));
        assertThat(response.getAlertActionManagerLargestQueueSize(), equalTo(1L));
    }

    @Test
    @TestLogging("alerts.actions:DEBUG")
    public void testBootStrapManyHistoryIndices() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        long numberOfAlertHistoryIndices = randomIntBetween(2,8);
        long numberOfAlertHistoryEntriesPerIndex = randomIntBetween(5,10);
        SearchRequest searchRequest = createConditionSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));

        for (int i = 0; i < numberOfAlertHistoryIndices; i++) {
            DateTime historyIndexDate = now.minus((new TimeValue(i, TimeUnit.DAYS)).getMillis());
            String actionHistoryIndex = HistoryStore.getAlertHistoryIndexNameForTime(historyIndexDate);
            createIndex(actionHistoryIndex);
            ensureGreen(actionHistoryIndex);
            logger.info("Created index {}", actionHistoryIndex);

            for (int j=0; j<numberOfAlertHistoryEntriesPerIndex; ++j){

                Alert alert = new Alert(
                        "action-test-"+ i + " " + j,
                        new CronSchedule("0/5 * * * * ? 2035"), //Set a cron schedule far into the future so this alert is never scheduled
                        new ScriptSearchCondition(logger, ScriptServiceProxy.of(scriptService()), ClientProxy.of(client()),
                                searchRequest, "return true", ScriptService.ScriptType.INLINE, "groovy"),
                        new SearchTransform(logger, ScriptServiceProxy.of(scriptService()), ClientProxy.of(client()), searchRequest),
                        new TimeValue(0),
                        new Actions(new ArrayList<Action>()),
                        null,
                        new Alert.Status()
                );
                XContentBuilder jsonBuilder = jsonBuilder();
                alert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

                PutAlertResponse putAlertResponse = alertClient().preparePutAlert(alert.name()).setAlertSource(jsonBuilder.bytes()).get();
                assertTrue(putAlertResponse.indexResponse().isCreated());

                FiredAlert firedAlert = new FiredAlert(alert, historyIndexDate, historyIndexDate);
                IndexResponse indexResponse = client().prepareIndex(actionHistoryIndex, HistoryStore.ALERT_HISTORY_TYPE, firedAlert.id())
                        .setConsistencyLevel(WriteConsistencyLevel.ALL)
                        .setSource(XContentFactory.jsonBuilder().value(firedAlert))
                        .get();
                assertTrue(indexResponse.isCreated());
            }
            client().admin().indices().prepareRefresh(actionHistoryIndex).get();
        }

        stopAlerting();
        startAlerting();
        AlertsStatsResponse response = alertClient().prepareAlertsStats().get();

        assertTrue(response.isAlertActionManagerStarted());
        assertThat(response.getAlertManagerStarted(), equalTo(AlertsService.State.STARTED));
        final long totalHistoryEntries = numberOfAlertHistoryEntriesPerIndex * numberOfAlertHistoryIndices ;

        assertBusy(new Runnable() {
            @Override
            public void run() {
                CountResponse countResponse = client().prepareCount(HistoryStore.ALERT_HISTORY_INDEX_PREFIX + "*")
                        .setTypes(HistoryStore.ALERT_HISTORY_TYPE)
                        .setQuery(QueryBuilders.termQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), FiredAlert.State.EXECUTED.id())).get();

                assertEquals(totalHistoryEntries, countResponse.getCount());
            }
        }, 30, TimeUnit.SECONDS);

    }


}
