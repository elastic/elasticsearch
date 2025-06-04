/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the different time fields in the watch_record are mapped as date types
 */
public class HistoryTemplateTimeMappingsTests extends AbstractWatcherIntegrationTestCase {

    public void testTimeFields() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("_logging", loggingAction("foobar"))
        ).get();

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().trigger("_id");

        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);
        assertBusy(() -> {
            GetMappingsResponse mappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT)
                .setIndicesOptions(IndicesOptions.strictExpandHidden())
                .get();
            assertThat(mappingsResponse, notNullValue());
            assertThat(mappingsResponse.getMappings().isEmpty(), is(false));
            for (var metadatas : mappingsResponse.getMappings().entrySet()) {
                if (metadatas.getKey().startsWith(HistoryStoreField.INDEX_PREFIX) == false) {
                    continue;
                }
                MappingMetadata metadata = metadatas.getValue();
                assertThat(metadata, notNullValue());
                try {
                    Map<String, Object> source = metadata.getSourceAsMap();
                    logger.info("checking index [{}] with metadata:\n[{}]", metadatas.getKey(), metadata.source().toString());
                    assertThat(extractValue("properties.trigger_event.properties.type.type", source), is((Object) "keyword"));
                    assertThat(extractValue("properties.trigger_event.properties.triggered_time.type", source), is((Object) "date"));
                    assertThat(
                        extractValue("properties.trigger_event.properties.schedule.properties.scheduled_time.type", source),
                        is((Object) "date")
                    );
                    assertThat(extractValue("properties.result.properties.execution_time.type", source), is((Object) "date"));
                } catch (ElasticsearchParseException e) {
                    throw new RuntimeException(e);
                }
            }
        });

    }
}
