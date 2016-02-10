/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.watcher.execution.ExecutionState;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.transform.TransformBuilders.scriptTransform;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the http host and path fields in the watch_record action result are
 * not analyzed so they can be used in aggregations
 */
@AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/724")
public class HistoryTemplateTransformMappingsTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean timeWarped() {
        return true; // just to have better control over the triggers
    }

    @Override
    protected boolean enableShield() {
        return false; // remove shield noise from this test
    }

    public void testTransformFields() throws Exception {
        String index = "the-index";
        String type = "the-type";
        createIndex(index);
        index(index, type, "{}");
        flush();
        refresh();

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id1").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .transform(scriptTransform("return [ 'key' : 'value1' ];"))
                .addAction("logger", scriptTransform("return [ 'key' : 'value2' ];"), loggingAction("indexed")))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().scheduler().trigger("_id1");

        // adding another watch which with a transform that should conflict with the preview watch. Since the
        // mapping for the transform construct is disabled, there should be nor problems.
        putWatchResponse = watcherClient().preparePutWatch("_id2").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .transform(scriptTransform("return [ 'key' : [ 'key1' : 'value1' ] ];"))
                .addAction("logger", scriptTransform("return [ 'key' : [ 'key1' : 'value2' ] ];"), loggingAction("indexed")))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().scheduler().trigger("_id2");

        flush();
        refresh();

        assertWatchWithMinimumActionsCount("_id1", ExecutionState.EXECUTED, 1);
        assertWatchWithMinimumActionsCount("_id2", ExecutionState.EXECUTED, 1);

        refresh();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings().get();
                assertThat(mappingsResponse, notNullValue());
                assertThat(mappingsResponse.getMappings().isEmpty(), is(false));
                for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> metadatas : mappingsResponse.getMappings()) {
                    if (!metadatas.key.startsWith(".watcher-history")) {
                        continue;
                    }
                    MappingMetaData metadata = metadatas.value.get("watch_record");
                    assertThat(metadata, notNullValue());
                    try {
                        Map<String, Object> source = metadata.getSourceAsMap();
                        logger.info("checking index [{}] with metadata:\n[{}]", metadatas.key, metadata.source().toString());
                        assertThat(extractValue("properties.result.properties.transform.properties.payload.enabled", source),
                                is((Object) false));

                        String path = "properties.result.properties.actions.properties.transform.properties.payload.enabled";
                        assertThat(extractValue(path, source), is((Object) false));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }
}
