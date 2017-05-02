/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingLevel;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.joda.time.DateTimeZone.UTC;

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
        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(templateRequest(searchSource().query(matchAllQuery()), "my-index")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                        .metadata(metadata))
                        .get();

        if (timeWarped()) {
            timeWarp().trigger("_name");
        } else {
            // Wait for a no action entry to be added. (the condition search request will not match, because there are no docs in my-index)
            assertWatchWithNoActionNeeded("_name", 1);
        }

        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                .setQuery(termQuery("metadata.foo", "bar"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));
    }

    public void testWatchMetadataAvailableAtExecution() throws Exception {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("logtext", "This is a test");

        LoggingAction.Builder loggingAction = loggingAction(new TextTemplate("_logging"))
                .setLevel(LoggingLevel.DEBUG)
                .setCategory("test");

        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 1 ? 2050")))
                        .input(searchInput(templateRequest(searchSource().query(matchAllQuery()), "my-index")))
                        .condition(AlwaysCondition.INSTANCE)
                        .addAction("testLogger", loggingAction)
                        .defaultThrottlePeriod(TimeValue.timeValueSeconds(0))
                        .metadata(metadata))
                .get();

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC));
        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch("_name")
                .setTriggerEvent(triggerEvent).setActionMode("_all", ActionExecutionMode.SIMULATE).get();
        Map<String, Object> result = executeWatchResponse.getRecordSource().getAsMap();
        logger.info("result=\n{}", result);

        assertThat(ObjectPath.<String>eval("metadata.foo", result), equalTo("bar"));
        assertThat(ObjectPath.<String>eval("result.actions.0.id", result), equalTo("testLogger"));
        assertThat(ObjectPath.<String>eval("result.actions.0.logging.logged_text", result), equalTo("_logging"));
    }
}
