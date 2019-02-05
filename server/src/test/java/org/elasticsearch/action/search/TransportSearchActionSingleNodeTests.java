/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class TransportSearchActionSingleNodeTests extends ESSingleNodeTestCase {

    public void testLocalClusterAlias() {
        long nowInMillis = System.currentTimeMillis();
        IndexRequest indexRequest = new IndexRequest("test");
        indexRequest.id("1");
        indexRequest.source("field", "value");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertEquals(RestStatus.CREATED, indexResponse.status());

        {
            SearchRequest searchRequest = new SearchRequest("local", nowInMillis);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertEquals(1, hits.length);
            SearchHit hit = hits[0];
            assertEquals("local", hit.getClusterAlias());
            assertEquals("test", hit.getIndex());
            assertEquals("1", hit.getId());
        }
        {
            SearchRequest searchRequest = new SearchRequest("", nowInMillis);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertEquals(1, hits.length);
            SearchHit hit = hits[0];
            assertEquals("", hit.getClusterAlias());
            assertEquals("test", hit.getIndex());
            assertEquals("1", hit.getId());
        }
    }

    public void testAbsoluteStartMillis() {
        {
            IndexRequest indexRequest = new IndexRequest("test-1970.01.01");
            indexRequest.id("1");
            indexRequest.source("date", "1970-01-01");
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            IndexResponse indexResponse = client().index(indexRequest).actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        {
            IndexRequest indexRequest = new IndexRequest("test-1982.01.01");
            indexRequest.id("1");
            indexRequest.source("date", "1982-01-01");
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            IndexResponse indexResponse = client().index(indexRequest).actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertEquals(2, searchResponse.getHits().getTotalHits().value);
        }
        {
            SearchRequest searchRequest = new SearchRequest("<test-{now/d}>");
            searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, true));
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertEquals(0, searchResponse.getTotalShards());
        }
        {
            SearchRequest searchRequest = new SearchRequest("", 0);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertEquals(2, searchResponse.getHits().getTotalHits().value);
        }
        {
            SearchRequest searchRequest = new SearchRequest("", 0);
            searchRequest.indices("<test-{now/d}>");
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            assertEquals("test-1970.01.01", searchResponse.getHits().getHits()[0].getIndex());
        }
        {
            SearchRequest searchRequest = new SearchRequest("", 0);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("date");
            rangeQuery.gte("1970-01-01");
            rangeQuery.lt("1982-01-01");
            sourceBuilder.query(rangeQuery);
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            assertEquals("test-1970.01.01", searchResponse.getHits().getHits()[0].getIndex());
        }
    }
}
