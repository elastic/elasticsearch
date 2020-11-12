/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.webhookAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the mapping for the watch_record are correct
 */
public class HistoryTemplateHttpMappingsTests extends AbstractWatcherIntegrationTestCase {

    private MockWebServer webServer = new MockWebServer();

    @Before
    public void init() throws Exception {
        webServer.start();
    }

    @After
    public void cleanup() throws Exception {
        webServer.close();
    }

    public void testHttpFields() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(httpInput(HttpRequestTemplate.builder("localhost", webServer.getPort()).path("/input/path")))
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("_webhook", webhookAction(HttpRequestTemplate.builder("localhost", webServer.getPort())
                        .path("/webhook/path")
                        .method(HttpMethod.POST)
                        .body("_body"))))
                .get();

        // one for the input, one for the webhook
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);

        SearchResponse response = client().prepareSearch(HistoryStoreField.INDEX_PREFIX_WITH_TEMPLATE + "*").setSource(searchSource()
                .aggregation(terms("input_result_path").field("result.input.http.request.path"))
                .aggregation(terms("input_result_host").field("result.input.http.request.host"))
                .aggregation(terms("webhook_path").field("result.actions.webhook.request.path")))
                .get();

        assertThat(response, notNullValue());
        assertThat(response.getHits().getTotalHits().value, is(1L));
        Aggregations aggs = response.getAggregations();
        assertThat(aggs, notNullValue());

        Terms terms = aggs.get("input_result_path");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey("/input/path"), notNullValue());
        assertThat(terms.getBucketByKey("/input/path").getDocCount(), is(1L));

        terms = aggs.get("webhook_path");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey("/webhook/path"), notNullValue());
        assertThat(terms.getBucketByKey("/webhook/path").getDocCount(), is(1L));

        assertThat(webServer.requests(), hasSize(2));
        assertThat(webServer.requests().get(0).getUri().getPath(), is("/input/path"));
        assertThat(webServer.requests().get(1).getUri().getPath(), is("/webhook/path"));
    }

    public void testExceptionMapping() {
        // delete all history indices to ensure that we only need to check a single index
        assertAcked(client().admin().indices().prepareDelete(HistoryStoreField.INDEX_PREFIX + "*"));

        String id = randomAlphaOfLength(10);
        // switch between delaying the input or the action http request
        boolean abortAtInput = randomBoolean();
        if (abortAtInput) {
            webServer.enqueue(new MockResponse().setBeforeReplyDelay(TimeValue.timeValueSeconds(5)));
        } else {
            webServer.enqueue(new MockResponse().setBody("{}"));
            webServer.enqueue(new MockResponse().setBeforeReplyDelay(TimeValue.timeValueSeconds(5)));
        }

        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), id).setSource(watchBuilder()
                .trigger(schedule(interval("1h")))
                .input(httpInput(HttpRequestTemplate.builder("localhost", webServer.getPort())
                        .path("/")
                        .readTimeout(abortAtInput ? TimeValue.timeValueMillis(10) : TimeValue.timeValueSeconds(10))))
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("_webhook", webhookAction(HttpRequestTemplate.builder("localhost", webServer.getPort())
                        .readTimeout(TimeValue.timeValueMillis(10))
                        .path("/webhook/path")
                        .method(HttpMethod.POST)
                        .body("_body"))))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));
        new ExecuteWatchRequestBuilder(client(), id).setRecordExecution(true).get();

        // ensure watcher history index has been written with this id
        flushAndRefresh(HistoryStoreField.INDEX_PREFIX + "*");
        SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.termQuery("watch_id", id))
                .get();
        assertHitCount(searchResponse, 1L);

        // ensure that enabled is set to false
        List<Boolean> indexed = new ArrayList<>();
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(HistoryStoreField.INDEX_PREFIX + "*").get();
        Iterator<MappingMetadata> iterator = mappingsResponse.getMappings().valuesIt();
        while (iterator.hasNext()) {
            MappingMetadata mapping = iterator.next();
            Map<String, Object> docMapping = mapping.getSourceAsMap();
            if (abortAtInput) {
                Boolean enabled = ObjectPath.eval("properties.result.properties.input.properties.error.enabled", docMapping);
                indexed.add(enabled);
            } else {
                Boolean enabled = ObjectPath.eval("properties.result.properties.actions.properties.error.enabled", docMapping);
                indexed.add(enabled);
            }
        }

        assertThat(indexed, hasSize(greaterThanOrEqualTo(1)));
        assertThat(indexed, hasItem(false));
        assertThat(indexed, not(hasItem(true)));
    }
}
