/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasIndex;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasProperty;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BulkRequestWithGlobalParametersIT extends ESRestHighLevelClientTestCase {

    public void testGlobalPipelineOnBulkRequest() throws IOException {
        createFieldAddingPipleine("xyz", "fieldNameXYZ", "valueXYZ");

        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("test").id("1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("test").id("2")
            .source(XContentType.JSON, "field", "bulk2"));
        request.pipeline("xyz");

        bulk(request);

        Iterable<SearchHit> hits = searchAll("test");
        assertThat(hits, containsInAnyOrder(hasId("1"), hasId("2")));
        assertThat(hits, everyItem(hasProperty(fieldFromSource("fieldNameXYZ"), equalTo("valueXYZ"))));
    }

    public void testPipelineOnRequestOverridesGlobalPipeline() throws IOException {
        createFieldAddingPipleine("globalId", "fieldXYZ", "valueXYZ");
        createFieldAddingPipleine("perIndexId", "someNewField", "someValue");

        BulkRequest request = new BulkRequest();
        request.pipeline("globalId");
        request.add(new IndexRequest("test").id("1")
            .source(XContentType.JSON, "field", "bulk1")
            .setPipeline("perIndexId"));
        request.add(new IndexRequest("test").id("2")
            .source(XContentType.JSON, "field", "bulk2")
            .setPipeline("perIndexId"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("test");
        assertThat(hits, everyItem(hasProperty(fieldFromSource("someNewField"), equalTo("someValue"))));
        // global pipeline was not applied
        assertThat(hits, everyItem(hasProperty(fieldFromSource("fieldXYZ"), nullValue())));
    }

    public void testMixPipelineOnRequestAndGlobal() throws IOException {
        createFieldAddingPipleine("globalId", "fieldXYZ", "valueXYZ");
        createFieldAddingPipleine("perIndexId", "someNewField", "someValue");

        // tag::bulk-request-mix-pipeline
        BulkRequest request = new BulkRequest();
        request.pipeline("globalId");

        request.add(new IndexRequest("test").id("1")
            .source(XContentType.JSON, "field", "bulk1")
            .setPipeline("perIndexId")); // <1>

        request.add(new IndexRequest("test").id("2")
            .source(XContentType.JSON, "field", "bulk2")); // <2>
        // end::bulk-request-mix-pipeline
        bulk(request);

        Iterable<SearchHit> hits = searchAll("test");
        assertThat(hits, containsInAnyOrder(
            both(hasId("1"))
                .and(hasProperty(fieldFromSource("someNewField"), equalTo("someValue"))),
            both(hasId("2"))
                .and(hasProperty(fieldFromSource("fieldXYZ"), equalTo("valueXYZ")))));
    }

    public void testGlobalIndex() throws IOException {
        BulkRequest request = new BulkRequest("global_index");
        request.add(new IndexRequest().id("1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest().id("2")
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("global_index");
        assertThat(hits, everyItem(hasIndex("global_index")));
    }

    @SuppressWarnings("unchecked")
    public void testIndexGlobalAndPerRequest() throws IOException {
        BulkRequest request = new BulkRequest("global_index");
        request.add(new IndexRequest("local_index").id("1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest().id("2") // will take global index
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("local_index", "global_index");
        assertThat(hits, containsInAnyOrder(
            both(hasId("1"))
                .and(hasIndex("local_index")),
            both(hasId("2"))
                .and(hasIndex("global_index"))));
    }

    public void testGlobalRouting() throws IOException {
        createIndexWithMultipleShards("index");
        BulkRequest request = new BulkRequest((String) null);
        request.add(new IndexRequest("index").id("1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("index").id("2")
            .source(XContentType.JSON, "field", "bulk1"));
        request.routing("1");
        bulk(request);

        Iterable<SearchHit> emptyHits = searchAll(new SearchRequest("index").routing("xxx"));
        assertThat(emptyHits, is(emptyIterable()));

        Iterable<SearchHit> hits = searchAll(new SearchRequest("index").routing("1"));
        assertThat(hits, containsInAnyOrder(hasId("1"), hasId("2")));
    }

    public void testMixLocalAndGlobalRouting() throws IOException {
        BulkRequest request = new BulkRequest((String) null);
        request.routing("globalRouting");
        request.add(new IndexRequest("index").id("1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("index").id( "2")
            .routing("localRouting")
            .source(XContentType.JSON, "field", "bulk1"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll(new SearchRequest("index").routing("globalRouting", "localRouting"));
        assertThat(hits, containsInAnyOrder(hasId("1"), hasId("2")));
    }

    private BulkResponse bulk(BulkRequest request) throws IOException {
        BulkResponse bulkResponse = execute(request, highLevelClient()::bulk, highLevelClient()::bulkAsync, RequestOptions.DEFAULT);
        assertFalse(bulkResponse.hasFailures());
        return bulkResponse;
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<SearchHit, T> fieldFromSource(String fieldName) {
        return (response) -> (T) response.getSourceAsMap().get(fieldName);
    }
}
