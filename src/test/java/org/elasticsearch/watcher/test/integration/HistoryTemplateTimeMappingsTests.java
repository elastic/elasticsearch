/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the different time fields in the watch_record are mapped as date types
 */
public class HistoryTemplateTimeMappingsTests extends AbstractWatcherIntegrationTests {

    @Override
    protected boolean timeWarped() {
        return true; // just to have better control over the triggers
    }

    @Override
    protected boolean enableShield() {
        return false; // remove shield noise from this test
    }

    @Test
    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-watcher/issues/351")
    public void testTimeFields() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .addAction("_logging", loggingAction("foobar")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().scheduler().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", WatchRecord.State.EXECUTED, 1);
        refresh();
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings().get();
        assertThat(mappingsResponse, notNullValue());
        assertThat(mappingsResponse.getMappings().isEmpty(), is(false));
        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> metadatas : mappingsResponse.getMappings()) {
            if (!metadatas.key.startsWith(".watch_history")) {
                continue;
            }
            MappingMetaData metadata = metadatas.value.get("watch_record");
            assertThat(metadata, notNullValue());
            Map<String, Object> source = metadata.getSourceAsMap();
            logger.info("metadata : [{}]", metadata.source().toString());
            assertThat(extractValue("properties.trigger_event.properties.schedule.properties.scheduled_time.type", source), is((Object) "date"));
            assertThat(extractValue("properties.trigger_event.properties.schedule.properties.triggered_time.type", source), is((Object) "date"));
            assertThat(extractValue("properties.watch_execution.properties.execution_time.type", source), is((Object) "date"));
        }
    }
}
