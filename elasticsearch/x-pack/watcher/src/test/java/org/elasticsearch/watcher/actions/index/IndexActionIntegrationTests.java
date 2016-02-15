/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.transform.TransformBuilders.scriptTransform;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/724")
public class IndexActionIntegrationTests extends AbstractWatcherIntegrationTestCase {
    public void testSimple() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(cron("0/1 * * * * ? 2020")))
                .input(simpleInput("foo", "bar"))
                .addAction("index-buckets", indexAction("idx", "type").setExecutionTimeField("@timestamp")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        DateTime now = timeWarped() ? timeWarp().clock().now(DateTimeZone.UTC) : DateTime.now(DateTimeZone.UTC);

        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch("_id")
                .setTriggerEvent(new ScheduleTriggerEvent(now, now))
                .get();

        assertThat(executeWatchResponse.getRecordSource().getValue("state"), is((Object) "executed"));

        flush("idx");
        refresh();

        SearchResponse searchResponse = client().prepareSearch("idx").setTypes("type").get();
        assertThat(searchResponse.getHits().totalHits(), is(1L));
        SearchHit hit = searchResponse.getHits().getAt(0);
        if (timeWarped()) {
            assertThat(hit.getSource(), hasEntry("@timestamp", (Object) WatcherDateTimeUtils.formatDate(now)));
        } else {
            assertThat(hit.getSource(), hasKey("@timestamp"));
            DateTime timestamp = WatcherDateTimeUtils.parseDate((String) hit.getSource().get("@timestamp"));
            assertThat(timestamp.isEqual(now) || timestamp.isAfter(now), is(true));
        }
        assertThat(hit.getSource(), hasEntry("foo", (Object) "bar"));
    }

    public void testSimpleWithDocField() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(cron("0/1 * * * * ? 2020")))
                .input(simpleInput("foo", "bar"))
                .addAction("index-buckets",
                        scriptTransform("return [ '_doc' : ctx.payload ]"),
                        indexAction("idx", "type").setExecutionTimeField("@timestamp")))

                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        DateTime now = timeWarped() ? timeWarp().clock().now(DateTimeZone.UTC) : DateTime.now(DateTimeZone.UTC);

        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch("_id")
                .setTriggerEvent(new ScheduleTriggerEvent(now, now))
                .get();

        assertThat(executeWatchResponse.getRecordSource().getValue("state"), is((Object) "executed"));

        flush("idx");
        refresh();

        SearchResponse searchResponse = client().prepareSearch("idx").setTypes("type").get();
        assertThat(searchResponse.getHits().totalHits(), is(1L));
        SearchHit hit = searchResponse.getHits().getAt(0);
        if (timeWarped()) {
            assertThat(hit.getSource(), hasEntry("@timestamp", (Object) WatcherDateTimeUtils.formatDate(now)));
        } else {
            assertThat(hit.getSource(), hasKey("@timestamp"));
            DateTime timestamp = WatcherDateTimeUtils.parseDate((String) hit.getSource().get("@timestamp"));
            assertThat(timestamp.isEqual(now) || timestamp.isAfter(now), is(true));
        }
        assertThat(hit.getSource(), hasEntry("foo", (Object) "bar"));
    }

    public void testSimpleWithDocFieldWrongFieldType() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(cron("0/1 * * * * ? 2020")))
                .input(simpleInput("foo", "bar"))
                .addAction("index-buckets",
                        scriptTransform("return [ '_doc' : 1 ]"),
                        indexAction("idx", "type").setExecutionTimeField("@timestamp")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        DateTime now = timeWarped() ? timeWarp().clock().now(DateTimeZone.UTC) : DateTime.now(DateTimeZone.UTC);

        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch("_id")
                .setTriggerEvent(new ScheduleTriggerEvent(now, now))
                .setRecordExecution(true)
                .get();

        assertThat(executeWatchResponse.getRecordSource().getValue("state"), is((Object) "executed"));

        flush();
        refresh();

        assertThat(client().admin().indices().prepareExists("idx").get().isExists(), is(false));

        assertThat(docCount(HistoryStore.INDEX_PREFIX + "*", HistoryStore.DOC_TYPE, searchSource()
                .query(matchQuery("result.actions.status", "failure"))), is(1L));

    }

    public void testIndexAggsBucketsAsDocuments() throws Exception {
        DateTime now = timeWarped() ? timeWarp().clock().now(DateTimeZone.UTC) : DateTime.now(DateTimeZone.UTC);
        long bucketCount = randomIntBetween(2, 5);
        for (int i = 0; i < bucketCount; i++) {
            index("idx", "type", jsonBuilder().startObject()
                    .field("timestamp", now.minusDays(i))
                    .endObject());
        }

        flush("idx");
        refresh();

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(cron("0/1 * * * * ? 2020")))
                .input(searchInput(new SearchRequest("idx")
                        .types("type")
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource()
                                .aggregation(dateHistogram("trend")
                                        .field("timestamp")
                                        .dateHistogramInterval(DateHistogramInterval.DAY)))))
                .addAction("index-buckets",

                        // this transform takes the bucket list and assigns it to `_doc`
                        // this means each bucket will be indexed as a separate doc,
                        // so we expect to have the same number of documents as the number
                        // of buckets.
                        scriptTransform("return [ '_doc' : ctx.payload.aggregations.trend.buckets]"),

                        indexAction("idx", "bucket").setExecutionTimeField("@timestamp")))

                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch("_id")
                .setTriggerEvent(new ScheduleTriggerEvent(now, now))
                .get();

        assertThat(executeWatchResponse.getRecordSource().getValue("state"), is((Object) "executed"));

        flush("idx");
        refresh();

        SearchResponse searchResponse = client().prepareSearch("idx").setTypes("bucket")
                .addSort("key", SortOrder.DESC)
                .get();
        assertThat(searchResponse.getHits().getTotalHits(), is(bucketCount));
        DateTime key = now.withMillisOfDay(0);
        int i = 0;
        for (SearchHit hit : searchResponse.getHits()) {
            if (timeWarped()) {
                assertThat(hit.getSource(), hasEntry("@timestamp", (Object) WatcherDateTimeUtils.formatDate(now)));
            } else {
                assertThat(hit.getSource(), hasKey("@timestamp"));
                DateTime timestamp = WatcherDateTimeUtils.parseDate((String) hit.getSource().get("@timestamp"));
                assertThat(timestamp.isEqual(now) || timestamp.isAfter(now), is(true));
            }
            assertThat(hit.getSource(), hasEntry("key", (Object) key.getMillis()));
            key = key.minusDays(1);
        }
    }
}
