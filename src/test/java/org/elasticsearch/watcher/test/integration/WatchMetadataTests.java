/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.watcher.actions.logging.LoggingAction;
import org.elasticsearch.watcher.actions.logging.LoggingLevel;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class WatchMetadataTests extends AbstractWatcherIntegrationTests {

    @Test
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
                        .input(searchInput(WatcherTestUtils.newInputSearchRequest("my-index").source(searchSource().query(matchAllQuery()))))
                        .condition(scriptCondition("ctx.payload.hits.total == 1"))
                        .metadata(metadata))
                        .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name");
        } else {
            // Wait for a no action entry to be added. (the condition search request will not match, because there are no docs in my-index)
            assertWatchWithNoActionNeeded("_name", 1);
        }

        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*")
                .setQuery(termQuery("metadata.foo", "bar"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));
    }

    @Test
    public void testWatchMetadataAvailableAtExecution() throws Exception {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("logtext", "This is a test");

        LoggingAction loggingAction = new LoggingAction(new Template("{{ctx.metadata.logtext}}"), LoggingLevel.DEBUG, "test");

        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(WatcherTestUtils.newInputSearchRequest("my-index").source(searchSource().query(matchAllQuery()))))
                        .condition(new AlwaysCondition())
                        .addAction("testLogger", loggingAction)
                        .metadata(metadata))
                .get();

        WatchRecord.Parser parser = getInstanceFromMaster(WatchRecord.Parser.class);
        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch("_name").addSimulatedActions("_all").get();

        WatchRecord record = parser.parse("test_run", 1, executeWatchResponse.getSource().getBytes());
        assertThat(record.metadata().get("foo").toString(), equalTo("bar"));
        assertThat(record.execution().actionsResults().get("testLogger").action(), instanceOf(LoggingAction.Result.Simulated.class));
        LoggingAction.Result.Simulated simulatedResult = (LoggingAction.Result.Simulated) (record.execution().actionsResults().get("testLogger").action());
        assertThat(simulatedResult.loggedText(), equalTo("This is a test"));
    }
}
