/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.watcher.scheduler.schedule.IntervalSchedule.Interval;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.transform.SearchTransform;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilder.watchSourceBuilder;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.scheduler.schedule.Schedules.interval;
import static org.elasticsearch.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;

/**
 */
public class TransformSearchTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testTransformSearchRequest() throws Exception {
        createIndex("my-condition-index", "my-payload-index", "my-payload-output");
        ensureGreen("my-condition-index", "my-payload-index", "my-payload-output");

        index("my-payload-index", "payload", "mytestresult");
        refresh();

        SearchRequest inputRequest = WatcherTestUtils.newInputSearchRequest("my-condition-index").source(searchSource().query(matchAllQuery()));
        SearchRequest transformRequest = WatcherTestUtils.newInputSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));
        transformRequest.searchType(SearchTransform.DEFAULT_SEARCH_TYPE);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("list", "baz");

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("test-payload")
                .source(watchSourceBuilder()
                        .schedule(interval(5, Interval.Unit.SECONDS))
                        .input(searchInput(inputRequest))
                        .transform(searchTransform(transformRequest))
                        .addAction(indexAction("my-payload-output", "result"))
                        .metadata(metadata)
                        .throttlePeriod(TimeValue.timeValueSeconds(0)))
                .get();
        assertThat(putWatchResponse.indexResponse().isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().scheduler().fire("test-payload");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("test-payload", 1, false);
        refresh();

        SearchRequest searchRequest = client().prepareSearch("my-payload-output").request();
        searchRequest.source(searchSource().query(matchAllQuery()));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));
        SearchHit hit = searchResponse.getHits().getHits()[0];
        String source = hit.getSourceRef().toUtf8();

        assertThat(source, containsString("mytestresult"));
    }
}
