/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;

public class HistoryTemplateTransformMappingsTests extends AbstractWatcherIntegrationTestCase {

    public void testTransformFields() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx")
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("foo")
                        .field("type", "object")
                        .field("enabled", false)
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(
                client().prepareIndex()
                    .setIndex("idx")
                    .setId("1")
                    .setSource(jsonBuilder().startObject().field("name", "first").field("foo", "bar").endObject())
            )
            .add(
                client().prepareIndex()
                    .setIndex("idx")
                    .setId("2")
                    .setSource(
                        jsonBuilder().startObject().field("name", "second").startObject("foo").field("what", "ever").endObject().endObject()
                    )
            )
            .get();

        new PutWatchRequestBuilder(client(), "_first").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput())
                .transform(searchTransform(templateRequest(searchSource().query(QueryBuilders.termQuery("name", "first")), "idx")))
                .addAction(
                    "logger",
                    searchTransform(templateRequest(searchSource().query(QueryBuilders.termQuery("name", "first")), "idx")),
                    loggingAction("indexed")
                )
        ).get();

        // execute another watch which with a transform that should conflict with the previous watch. Since the
        // mapping for the transform construct is disabled, there should be no problems.
        new PutWatchRequestBuilder(client(), "_second").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput())
                .transform(searchTransform(templateRequest(searchSource().query(QueryBuilders.termQuery("name", "second")), "idx")))
                .addAction(
                    "logger",
                    searchTransform(templateRequest(searchSource().query(QueryBuilders.termQuery("name", "second")), "idx")),
                    loggingAction("indexed")
                )
        ).get();

        new ExecuteWatchRequestBuilder(client(), "_first").setRecordExecution(true).get();
        new ExecuteWatchRequestBuilder(client(), "_second").setRecordExecution(true).get();

        assertBusy(() -> {
            GetFieldMappingsResponse response = client().admin()
                .indices()
                .prepareGetFieldMappings(".watcher-history*")
                .setFields("result.actions.transform.payload")
                .includeDefaults(true)
                .get();

            // time might have rolled over to a new day, thus we need to check that this field exists only in one of the history indices
            Optional<GetFieldMappingsResponse.FieldMappingMetadata> mapping = response.mappings()
                .values()
                .stream()
                .map(map -> map.get("result.actions.transform.payload"))
                .filter(Objects::nonNull)
                .findFirst();

            assertTrue(mapping.isEmpty());
        });
    }
}
