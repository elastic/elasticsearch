/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingLevel;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.noneInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class WatchMetadataTests extends AbstractWatcherIntegrationTestCase {

    public void testWatchMetadata() throws Exception {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        List<String> metaList = new ArrayList<>();
        metaList.add("this");
        metaList.add("is");
        metaList.add("a");
        metaList.add("test");

        metadata.put("baz", metaList);
        new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(
                watchBuilder().trigger(schedule(cron("0/5 * * * * ? *")))
                    .input(noneInput())
                    .condition(new CompareCondition("ctx.payload.hits.total.value", CompareCondition.Op.EQ, 1L))
                    .metadata(metadata)
            )
            .get();

        timeWarp().trigger("_name");

        assertBusy(() -> {
            refresh();
            SearchResponse searchResponse;
            try {
                searchResponse = prepareSearch(HistoryStoreField.DATA_STREAM + "*").setQuery(termQuery("metadata.foo", "bar")).get();
            } catch (SearchPhaseExecutionException e) {
                if (e.getCause() instanceof NoShardAvailableActionException) {
                    // Nothing has created the index yet
                    searchResponse = null;
                } else {
                    throw e;
                }
            }
            assertNotNull(searchResponse);
            try {
                assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            } finally {
                searchResponse.decRef();
            }
        });
    }

    public void testWatchMetadataAvailableAtExecution() throws Exception {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("logtext", "This is a test");

        LoggingAction.Builder loggingAction = loggingAction(new TextTemplate("_logging")).setLevel(LoggingLevel.DEBUG).setCategory("test");

        new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(
                watchBuilder().trigger(schedule(cron("0 0 0 1 1 ? 2050")))
                    .input(noneInput())
                    .condition(InternalAlwaysCondition.INSTANCE)
                    .addAction("testLogger", loggingAction)
                    .defaultThrottlePeriod(TimeValue.timeValueSeconds(0))
                    .metadata(metadata)
            )
            .get();

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC));
        ExecuteWatchResponse executeWatchResponse = new ExecuteWatchRequestBuilder(client()).setId("_name")
            .setTriggerEvent(triggerEvent)
            .setActionMode("_all", ActionExecutionMode.SIMULATE)
            .get();
        Map<String, Object> result = executeWatchResponse.getRecordSource().getAsMap();
        logger.info("result=\n{}", result);

        assertThat(ObjectPath.<String>eval("metadata.foo", result), equalTo("bar"));
        assertThat(ObjectPath.<String>eval("result.actions.0.id", result), equalTo("testLogger"));
        assertThat(ObjectPath.<String>eval("result.actions.0.logging.logged_text", result), equalTo("_logging"));
    }
}
