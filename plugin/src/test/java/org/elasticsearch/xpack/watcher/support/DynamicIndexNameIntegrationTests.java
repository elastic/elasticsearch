/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.joda.time.DateTimeZone.UTC;

public class DynamicIndexNameIntegrationTests extends AbstractWatcherIntegrationTestCase {

    private final DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY.MM.dd");

    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Override
    protected boolean enableSecurity() {
        return false; // reduce noise
    }

    public void testDynamicIndexAction() throws Exception {
        final String indexName = "idx-" + formatter.print(new DateTime(timeWarp().clock().millis(), UTC));
        WatcherClient watcherClient = watcherClient();
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(simpleInput("key", "value"))
                        .condition(AlwaysCondition.INSTANCE)
                        .addAction("dynamic_index", indexAction("<idx-{now}>", "type")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_id");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_id", 1, false);

        logger.info("checking index [{}]", indexName);
        assertBusy(() -> {
            flush();
            refresh();
            long docCount = docCount(indexName, "type", matchAllQuery());
            assertThat(docCount, is(1L));
        });
    }

    public void testDynamicIndexSearchInput() throws Exception {
        final String indexName = "idx-" + formatter.print(new DateTime(timeWarp().clock().millis(), UTC));
        createIndex(indexName);
        index(indexName, "type", "1", "key", "value");
        flush();
        refresh();

        String indexNameDateMathExpressions = "<idx-{now/d}>";
        WatcherClient watcherClient = watcherClient();
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(templateRequest(new SearchSourceBuilder(), indexNameDateMathExpressions))))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_id");
        flush();
        refresh();

        SearchResponse response = searchHistory(searchSource().query(matchQuery("result.input.search.request.indices",
                indexNameDateMathExpressions)));
        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    public void testDynamicIndexSearchTransform() throws Exception {
        final String indexName = "idx-" + formatter.print(new DateTime(timeWarp().clock().millis(), UTC));
        createIndex(indexName);
        index(indexName, "type", "1", "key", "value");
        flush();
        refresh();

        final String indexNameDateMathExpressions = "<idx-{now/d}>";
        WatcherClient watcherClient = watcherClient();
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .transform(searchTransform(templateRequest(new SearchSourceBuilder(), indexNameDateMathExpressions)))
                        .addAction("log", loggingAction("heya")))
                        .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_id");
        flush();
        refresh();

        SearchResponse response = searchWatchRecords(builder ->
                builder.setQuery(matchQuery("result.transform.search.request.indices", indexNameDateMathExpressions)));
        assertThat(response.getHits().getTotalHits(), is(1L));
    }
}
