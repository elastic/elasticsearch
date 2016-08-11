/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.watcher.input.Input;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.chainInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.newInputSearchRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;

public class HistoryIntegrationTests extends AbstractWatcherIntegrationTestCase {

    // issue: https://github.com/elastic/x-plugins/issues/2338
    public void testThatHistoryIsWrittenWithChainedInput() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject().startObject("inner").field("date", "2015-06-06").endObject()
                .endObject();
        index("foo", "bar", "1", xContentBuilder);
        refresh();

        WatchSourceBuilder builder = watchBuilder()
                .trigger(schedule(interval("10s")))
                .addAction("logging", loggingAction("foo"));

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("foo").addSort(SortBuilders.fieldSort("inner.date").order
                (SortOrder.DESC));
        builder.input(chainInput().add("first", searchInput(searchRequestBuilder.request())));

        PutWatchResponse response = watcherClient().preparePutWatch("test_watch").setSource(builder).get();
        assertThat(response.isCreated(), is(true));

        watcherClient().prepareExecuteWatch("test_watch").setRecordExecution(true).get();

        flushAndRefresh(".watcher-history-*");
        SearchResponse searchResponse = client().prepareSearch(".watcher-history-*").get();
        assertHitCount(searchResponse, 1);
    }

    // See https://github.com/elastic/x-plugins/issues/2913
    public void testFailedInputResultWithDotsInFieldNameGetsStored() throws Exception {
        SearchRequest sortSearchRequest = newInputSearchRequest("non-existing-index")
                .source(searchSource()
                        .query(matchAllQuery())
                        .sort("trigger_event.triggered_time", SortOrder.DESC)
                        .size(1));

        // The result of the search input will be a failure, because a missing index does not exist when
        // the query is executed
        Input.Builder input = searchInput(sortSearchRequest);
        // wrapping this randomly into a chained input to test this as well
        boolean useChained = randomBoolean();
        if (useChained) {
            input = chainInput().add("chained", input);
        }

        watcherClient().preparePutWatch("test_watch")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.HOURS)))
                        .input(input)
                        .addAction("_logger", loggingAction("#### randomLogging")))
                .get();

        watcherClient().prepareExecuteWatch("test_watch").setRecordExecution(true).get();

        flush(".watcher-history*");
        SearchResponse searchResponse = client().prepareSearch(".watcher-history*").setSize(0).get();
        assertHitCount(searchResponse, 1);

        // as fields with dots are allowed in 5.0 again, the mapping must be checked in addition
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(".watcher-history*").addTypes("watch_record").get();
        byte[] bytes = response.getMappings().values().iterator().next().value.get("watch_record").source().uncompressed();
        XContentSource source = new XContentSource(new BytesArray(bytes), XContentType.JSON);
        // lets make sure the body fields are disabled
        if (useChained) {
            String chainedPath = "watch_record.properties.result.properties.input.properties.chain.properties.chained.properties.search" +
                    ".properties.request.properties.body.enabled";
            assertThat(source.getValue(chainedPath), is(false));
        } else {
            String path =
                    "watch_record.properties.result.properties.input.properties.search.properties.request.properties.body.enabled";
            assertThat(source.getValue(path), is(false));
        }
    }

    // See https://github.com/elastic/x-plugins/issues/2913
    public void testPayloadInputWithDotsInFieldNameWorks() throws Exception {
        Input.Builder input = simpleInput("foo.bar", "bar");

        // wrapping this randomly into a chained input to test this as well
        boolean useChained = randomBoolean();
        if (useChained) {
            input = chainInput().add("chained", input);
        }

        watcherClient().preparePutWatch("test_watch")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.HOURS)))
                        .input(input)
                        .addAction("_logger", loggingAction("#### randomLogging")))
                .get();

        watcherClient().prepareExecuteWatch("test_watch").setRecordExecution(true).get();

        flush(".watcher-history*");
        SearchResponse searchResponse = client().prepareSearch(".watcher-history*").setSize(0).get();
        assertHitCount(searchResponse, 1);

        // as fields with dots are allowed in 5.0 again, the mapping must be checked in addition
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(".watcher-history*").addTypes("watch_record").get();
        byte[] bytes = response.getMappings().values().iterator().next().value.get("watch_record").source().uncompressed();
        XContentSource source = new XContentSource(new BytesArray(bytes), XContentType.JSON);

        // lets make sure the body fields are disabled
        if (useChained) {
            String path = "watch_record.properties.result.properties.input.properties.chain.properties.chained.properties.payload.enabled";
            assertThat(source.getValue(path), is(false));
        } else {
            String path = "watch_record.properties.result.properties.input.properties.payload.enabled";
            assertThat(source.getValue(path), is(false));
        }

    }
}
