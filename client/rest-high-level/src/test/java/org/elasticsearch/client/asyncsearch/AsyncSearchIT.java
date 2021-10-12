/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class AsyncSearchIT extends ESRestHighLevelClientTestCase {

    public void testAsyncSearch() throws IOException {
        String index = "test-index";
        createIndex(index, Settings.EMPTY);
        BulkRequest bulkRequest = new BulkRequest()
            .add(new IndexRequest(index).id("1").source(Collections.singletonMap("foo", "bar"), XContentType.JSON))
            .add(new IndexRequest(index).id("2").source(Collections.singletonMap("foo", "bar2"), XContentType.JSON))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertEquals(
            RestStatus.OK,
            highLevelClient().bulk(
                bulkRequest,
                RequestOptions.DEFAULT
            ).status()
        );

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .aggregation(AggregationBuilders.terms("1").field("foo.keyword"));
        SubmitAsyncSearchRequest submitRequest = new SubmitAsyncSearchRequest(sourceBuilder, index);
        submitRequest.setKeepOnCompletion(true);
        AsyncSearchResponse submitResponse = highLevelClient().asyncSearch().submit(submitRequest, RequestOptions.DEFAULT);
        assertNotNull(submitResponse.getId());
        assertFalse(submitResponse.isPartial());
        assertTrue(submitResponse.getStartTime() > 0);
        assertTrue(submitResponse.getExpirationTime() > 0);
        assertNotNull(submitResponse.getSearchResponse());
        assertThat(submitResponse.getSearchResponse().getHits().getTotalHits().value, equalTo(2));
        StringTerms terms = submitResponse.getSearchResponse().getAggregations().get("1");
        assertThat(terms.getBuckets().size(), equalTo(2));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), equalTo("bar"));
        assertThat(terms.getBuckets().get(0).getDocCount(), equalTo(1L));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), equalTo("bar2"));
        assertThat(terms.getBuckets().get(1).getDocCount(), equalTo(1L));

        if (submitResponse.isRunning() == false) {
            assertFalse(submitResponse.isPartial());
        } else {
            assertTrue(submitResponse.isPartial());
        }

        GetAsyncSearchRequest getRequest = new GetAsyncSearchRequest(submitResponse.getId());
        AsyncSearchResponse getResponse = highLevelClient().asyncSearch().get(getRequest, RequestOptions.DEFAULT);
        while (getResponse.isRunning()) {
            getResponse = highLevelClient().asyncSearch().get(getRequest, RequestOptions.DEFAULT);
        }

        assertFalse(getResponse.isRunning());
        assertFalse(getResponse.isPartial());
        assertTrue(getResponse.getStartTime() > 0);
        assertTrue(getResponse.getExpirationTime() > 0);
        assertEquals(getResponse.getSearchResponse(), submitResponse.getSearchResponse());

        DeleteAsyncSearchRequest deleteRequest = new DeleteAsyncSearchRequest(submitResponse.getId());
        AcknowledgedResponse deleteAsyncSearchResponse = highLevelClient().asyncSearch().delete(deleteRequest,
                RequestOptions.DEFAULT);
        assertNotNull(deleteAsyncSearchResponse);
        assertNotNull(deleteAsyncSearchResponse.isAcknowledged());
    }
}
