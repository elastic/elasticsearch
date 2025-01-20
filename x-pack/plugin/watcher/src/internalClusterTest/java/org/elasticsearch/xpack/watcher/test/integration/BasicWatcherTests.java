/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.QueryWatchesAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedules;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.MonthTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.WeekTimes;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.storedscripts.StoredScriptIntegTestUtils.putJsonStoredScript;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.daily;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.hourly;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.monthly;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.weekly;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BasicWatcherTests extends AbstractWatcherIntegrationTestCase {

    public void testIndexWatch() throws Exception {
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        prepareIndex("idx").setSource("field", "foo").get();
        refresh();
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "foo")), "idx");
        new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(
                watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                    .input(searchInput(request))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                    .addAction("_logger", loggingAction("_logging").setCategory("_category"))
            )
            .get();

        timeWarp().trigger("_name");
        assertWatchWithMinimumPerformedActionsCount("_name", 1);

        GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client()).setId("_name").get();
        assertThat(getWatchResponse.isFound(), is(true));
        assertThat(getWatchResponse.getSource(), notNullValue());
    }

    public void testIndexWatchRegisterWatchBeforeTargetIndex() throws Exception {
        WatcherSearchTemplateRequest searchRequest = templateRequest(searchSource().query(termQuery("field", "value")), "idx");
        new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(
                watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                    .input(searchInput(searchRequest))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
            )
            .get();
        timeWarp().trigger("_name");
        // The watch's condition won't meet because there is no data that matches with the query
        assertWatchWithNoActionNeeded("_name", 1);

        // Index sample doc after we register the watch and the watch's condition should meet
        prepareIndex("idx").setSource("field", "value").get();
        refresh();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().trigger("_name");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_name", 1);
    }

    public void testDeleteWatch() throws Exception {
        WatcherSearchTemplateRequest searchRequest = templateRequest(searchSource().query(matchAllQuery()), "idx");
        PutWatchResponse indexResponse = new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(
                watchBuilder().trigger(schedule(cron("0/1 * * * * ? 2020")))
                    .input(searchInput(searchRequest))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
            )
            .get();
        assertThat(indexResponse.isCreated(), is(true));
        DeleteWatchResponse deleteWatchResponse = new DeleteWatchRequestBuilder(client()).setId("_name").get();
        assertThat(deleteWatchResponse, notNullValue());
        assertThat(deleteWatchResponse.isFound(), is(true));

        refresh();
        assertHitCount(prepareSearch(Watch.INDEX).setSize(0), 0L);

        // Deleting the same watch for the second time
        deleteWatchResponse = new DeleteWatchRequestBuilder(client()).setId("_name").get();
        assertThat(deleteWatchResponse, notNullValue());
        assertThat(deleteWatchResponse.isFound(), is(false));
    }

    public void testMalformedWatch() throws Exception {
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        prepareIndex("idx").setSource("field", "value").get();
        XContentBuilder watchSource = jsonBuilder();

        watchSource.startObject();
        watchSource.field("unknown_field", "x");
        watchSource.startObject("schedule").field("cron", "0/5 * * * * ? *").endObject();

        watchSource.startObject("condition").startObject("script").field("script", "return true");
        watchSource.field("request", templateRequest(searchSource().query(matchAllQuery())));
        watchSource.endObject().endObject();

        watchSource.endObject();
        try {
            new PutWatchRequestBuilder(client()).setId("_name")
                .setSource(BytesReference.bytes(watchSource), watchSource.contentType())
                .get();
            fail();
        } catch (ElasticsearchParseException e) {
            // In watch store we fail parsing if an watch contains undefined fields.
        }
        try {
            prepareIndex(Watch.INDEX).setId("_name").setSource(watchSource).get();
            fail();
        } catch (Exception e) {
            // The watch index template the mapping is defined as strict
        }
    }

    public void testModifyWatches() throws Exception {
        createIndex("idx");
        WatcherSearchTemplateRequest searchRequest = templateRequest(searchSource().query(matchAllQuery()), "idx");

        WatchSourceBuilder source = watchBuilder().trigger(schedule(interval("5s")))
            .input(searchInput(searchRequest))
            .addAction("_id", indexAction("idx"));

        new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(source.condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
            .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().trigger("_name");
        assertWatchWithMinimumPerformedActionsCount("_name", 0, false);

        new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(source.condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 0L)))
            .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().trigger("_name");
        refresh();
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);

        new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(
                source.trigger(schedule(Schedules.cron("0/1 * * * * ? 2020")))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 0L))
            )
            .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().trigger("_name");
        long count = findNumberOfPerformedActions("_name");

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().trigger("_name");
        assertThat(count, equalTo(findNumberOfPerformedActions("_name")));
    }

    public void testConditionSearchWithSource() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(matchQuery("level", "a"));
        testConditionSearch(templateRequest(searchSourceBuilder, "events"));
    }

    public void testConditionSearchWithIndexedTemplate() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(matchQuery("level", "a"));
        putJsonStoredScript(
            "my-template",
            BytesReference.bytes(
                jsonBuilder().startObject()
                    .startObject("script")
                    .field("lang", "mustache")
                    .field("source")
                    .value(searchSourceBuilder)
                    .endObject()
                    .endObject()
            )
        );

        Script template = new Script(ScriptType.STORED, null, "my-template", Collections.emptyMap());
        WatcherSearchTemplateRequest searchRequest = new WatcherSearchTemplateRequest(
            new String[] { "events" },
            SearchType.DEFAULT,
            WatcherSearchTemplateRequest.DEFAULT_INDICES_OPTIONS,
            template
        );
        testConditionSearch(searchRequest);
    }

    public void testInputFiltering() throws Exception {
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        prepareIndex("idx").setSource(jsonBuilder().startObject().field("field", "foovalue").endObject()).get();
        refresh();
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "foovalue")), "idx");
        new PutWatchRequestBuilder(client()).setId("_name1")
            .setSource(
                watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                    .input(searchInput(request).extractKeys("hits.total.value"))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
            )
            .get();
        // in this watcher the condition will fail, because max_score isn't extracted, only total:
        new PutWatchRequestBuilder(client()).setId("_name2")
            .setSource(
                watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                    .input(searchInput(request).extractKeys("hits.total.value"))
                    .condition(new CompareCondition("ctx.payload.hits.max_score", CompareCondition.Op.GTE, 0L))
            )
            .get();

        timeWarp().trigger("_name1");
        assertWatchWithMinimumPerformedActionsCount("_name1", 1);
        timeWarp().trigger("_name2");
        assertWatchWithNoActionNeeded("_name2", 1);

        // Check that the input result payload has been filtered
        refresh();
        SearchResponse searchResponse = searchWatchRecords(builder -> builder.setQuery(matchQuery("watch_id", "_name1")));
        try {
            assertHitCount(searchResponse, 1);
            XContentSource source = xContentSource(searchResponse.getHits().getAt(0).getSourceRef());
            assertThat(source.getValue("result.input.payload.hits.total"), equalTo((Object) 1));
        } finally {
            searchResponse.decRef();
        }
    }

    public void testPutWatchWithNegativeSchedule() throws Exception {
        try {
            new PutWatchRequestBuilder(client()).setId("_name")
                .setSource(
                    watchBuilder().trigger(schedule(interval(-5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(simpleInput("key", "value"))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_logger", loggingAction("executed!"))
                )
                .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("interval can't be lower than 1000 ms, but [-5s] was specified"));
        }

        try {
            new PutWatchRequestBuilder(client()).setId("_name")
                .setSource(
                    watchBuilder().trigger(schedule(hourly().minutes(-10).build()))
                        .input(simpleInput("key", "value"))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_logger", loggingAction("executed!"))
                )
                .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("invalid hourly minute [-10]. minute must be between 0 and 59 incl."));
        }

        try {
            new PutWatchRequestBuilder(client()).setId("_name")
                .setSource(
                    watchBuilder().trigger(schedule(daily().atRoundHour(-10).build()))
                        .input(simpleInput("key", "value"))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_logger", loggingAction("executed!"))
                )
                .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("invalid time [0-10:00]. invalid time hour value [-10]. time hours must be between 0 and 23 incl.")
            );
        }

        try {
            new PutWatchRequestBuilder(client()).setId("_name")
                .setSource(
                    watchBuilder().trigger(schedule(weekly().time(WeekTimes.builder().atRoundHour(-10).build()).build()))
                        .input(simpleInput("key", "value"))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_logger", loggingAction("executed!"))
                )
                .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("invalid time [0-10:00]. invalid time hour value [-10]. time hours must be between 0 and 23 incl.")
            );
        }

        try {
            new PutWatchRequestBuilder(client()).setId("_name")
                .setSource(
                    watchBuilder().trigger(schedule(monthly().time(MonthTimes.builder().atRoundHour(-10).build()).build()))
                        .input(simpleInput("key", "value"))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_logger", loggingAction("executed!"))
                )
                .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("invalid time [0-10:00]. invalid time hour value [-10]. time hours must be between 0 and 23 incl.")
            );
        }
    }

    private void testConditionSearch(WatcherSearchTemplateRequest request) throws Exception {
        // reset, so we don't miss event docs when we filter over the _timestamp field.
        timeWarp().clock().setTime(ZonedDateTime.now(Clock.systemUTC()));

        String watchName = "_name";
        assertAcked(prepareCreate("events").setMapping("level", "type=text"));

        new PutWatchRequestBuilder(client()).setId(watchName)
            .setSource(
                watchBuilder().trigger(schedule(interval("5s")))
                    .input(searchInput(request))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GTE, 3L))
            )
            .get();

        logger.info("created watch [{}] at [{}]", watchName, ZonedDateTime.now(Clock.systemUTC()));

        prepareIndex("events").setSource("level", "a").get();
        prepareIndex("events").setSource("level", "a").get();

        refresh();
        timeWarp().clock().fastForwardSeconds(1);
        timeWarp().trigger(watchName);
        assertWatchWithNoActionNeeded(watchName, 1);

        prepareIndex("events").setSource("level", "b").get();
        refresh();
        timeWarp().clock().fastForwardSeconds(1);
        timeWarp().trigger(watchName);
        assertWatchWithNoActionNeeded(watchName, 2);

        prepareIndex("events").setSource("level", "a").get();
        refresh();
        timeWarp().clock().fastForwardSeconds(1);
        timeWarp().trigger(watchName);
        assertWatchWithMinimumPerformedActionsCount(watchName, 1);
    }

    public void testQueryWatches() {
        int numWatches = 6;
        for (int i = 0; i < numWatches; i++) {
            PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId("" + i)
                .setSource(
                    watchBuilder().trigger(schedule(interval(1, IntervalSchedule.Interval.Unit.DAYS)))
                        .addAction("_logger", loggingAction("log me"))
                        .metadata(Map.of("key1", i, "key2", numWatches - i))
                )
                .get();
            assertThat(putWatchResponse.isCreated(), is(true));
        }
        refresh();

        QueryWatchesAction.Request request = new QueryWatchesAction.Request(
            0,
            2,
            null,
            List.of(new FieldSortBuilder("metadata.key1")),
            null
        );
        QueryWatchesAction.Response response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo((long) numWatches));
        assertThat(response.getWatches().size(), equalTo(2));
        assertThat(response.getWatches().get(0).getId(), equalTo("0"));
        Map<?, ?> watcherMetadata = (Map<?, ?>) response.getWatches().get(0).getSource().getAsMap().get("metadata");
        assertThat(watcherMetadata.get("key2"), equalTo(6));
        assertThat(response.getWatches().get(1).getId(), equalTo("1"));
        watcherMetadata = (Map<?, ?>) response.getWatches().get(1).getSource().getAsMap().get("metadata");
        assertThat(watcherMetadata.get("key2"), equalTo(5));

        request = new QueryWatchesAction.Request(2, 2, null, List.of(new FieldSortBuilder("metadata.key1")), null);
        response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo((long) numWatches));
        assertThat(response.getWatches().size(), equalTo(2));
        assertThat(response.getWatches().get(0).getId(), equalTo("2"));
        watcherMetadata = (Map<?, ?>) response.getWatches().get(0).getSource().getAsMap().get("metadata");
        assertThat(watcherMetadata.get("key2"), equalTo(4));
        assertThat(response.getWatches().get(1).getId(), equalTo("3"));
        watcherMetadata = (Map<?, ?>) response.getWatches().get(1).getSource().getAsMap().get("metadata");
        assertThat(watcherMetadata.get("key2"), equalTo(3));

        request = new QueryWatchesAction.Request(null, null, new TermQueryBuilder("_id", "4"), null, null);
        response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo(1L));
        assertThat(response.getWatches().size(), equalTo(1));
        assertThat(response.getWatches().get(0).getId(), equalTo("4"));
        watcherMetadata = (Map<?, ?>) response.getWatches().get(0).getSource().getAsMap().get("metadata");
        assertThat(watcherMetadata.get("key1"), equalTo(4));
        assertThat(watcherMetadata.get("key2"), equalTo(2));

        request = new QueryWatchesAction.Request(4, 2, null, List.of(new FieldSortBuilder("metadata.key2")), null);
        response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo((long) numWatches));
        assertThat(response.getWatches().size(), equalTo(2));
        assertThat(response.getWatches().get(0).getId(), equalTo("1"));
        watcherMetadata = (Map<?, ?>) response.getWatches().get(0).getSource().getAsMap().get("metadata");
        assertThat(watcherMetadata.get("key1"), equalTo(1));
        assertThat(watcherMetadata.get("key2"), equalTo(5));
        assertThat(response.getWatches().get(1).getId(), equalTo("0"));
        watcherMetadata = (Map<?, ?>) response.getWatches().get(1).getSource().getAsMap().get("metadata");
        assertThat(watcherMetadata.get("key1"), equalTo(0));
        assertThat(watcherMetadata.get("key2"), equalTo(6));
    }

    public void testQueryWatchesNoWatches() {
        QueryWatchesAction.Request request = new QueryWatchesAction.Request(null, null, null, null, null);
        QueryWatchesAction.Response response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo(0L));
        assertThat(response.getWatches().size(), equalTo(0));

        // Even if there is no .watches index this api should work and return 0 watches.
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
        deleteIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpenHidden());
        indicesAdmin().delete(deleteIndexRequest).actionGet();
        request = new QueryWatchesAction.Request(null, null, null, null, null);
        response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo(0L));
        assertThat(response.getWatches().size(), equalTo(0));
    }

    public void testQueryWatchesSearchAfter() {
        int numWatches = 6;
        for (int i = 0; i < numWatches; i++) {
            PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId("" + i)
                .setSource(
                    watchBuilder().trigger(schedule(interval(1, IntervalSchedule.Interval.Unit.DAYS)))
                        .addAction("_logger", loggingAction("log me"))
                        .metadata(Map.of("_id", i))
                )
                .get();
            assertThat(putWatchResponse.isCreated(), is(true));
        }
        refresh();

        QueryWatchesAction.Request request = new QueryWatchesAction.Request(
            0,
            2,
            null,
            List.of(new FieldSortBuilder("metadata._id")),
            null
        );
        QueryWatchesAction.Response response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo((long) numWatches));
        assertThat(response.getWatches().size(), equalTo(2));
        assertThat(response.getWatches().get(0).getId(), equalTo("0"));
        assertThat(response.getWatches().get(1).getId(), equalTo("1"));

        request = new QueryWatchesAction.Request(
            0,
            2,
            null,
            List.of(new FieldSortBuilder("metadata._id")),
            new SearchAfterBuilder().setSortValues(new Object[] { "1" })
        );
        response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo((long) numWatches));
        assertThat(response.getWatches().size(), equalTo(2));
        assertThat(response.getWatches().get(0).getId(), equalTo("2"));
        assertThat(response.getWatches().get(1).getId(), equalTo("3"));

        request = new QueryWatchesAction.Request(
            0,
            2,
            null,
            List.of(new FieldSortBuilder("metadata._id")),
            new SearchAfterBuilder().setSortValues(new Object[] { "3" })
        );
        response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo((long) numWatches));
        assertThat(response.getWatches().size(), equalTo(2));
        assertThat(response.getWatches().get(0).getId(), equalTo("4"));
        assertThat(response.getWatches().get(1).getId(), equalTo("5"));

        request = new QueryWatchesAction.Request(
            0,
            2,
            null,
            List.of(new FieldSortBuilder("metadata._id")),
            new SearchAfterBuilder().setSortValues(new Object[] { "5" })
        );
        response = client().execute(QueryWatchesAction.INSTANCE, request).actionGet();
        assertThat(response.getWatchTotalCount(), equalTo((long) numWatches));
        assertThat(response.getWatches().size(), equalTo(0));
    }
}
