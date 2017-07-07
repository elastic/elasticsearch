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

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.greaterThan;

/**
 * This class is used to generate the Java High Level REST Client Search API documentation.
 * <p>
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 * <p>
 * Where example is your tag name.
 * <p>
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/SearchDocumentationIT.java[example]
 * --------------------------------------------------
 */
public class SearchDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testScroll() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest("posts", "doc", "1")
                    .source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?"));
            request.add(new IndexRequest("posts", "doc", "2")
                    .source(XContentType.JSON, "title", "Current status and upcoming changes in Elasticsearch"));
            request.add(new IndexRequest("posts", "doc", "3")
                    .source(XContentType.JSON, "title", "The Future of Federated Search in Elasticsearch"));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            BulkResponse bulkResponse = client.bulk(request);
            assertSame(bulkResponse.status(), RestStatus.OK);
            assertFalse(bulkResponse.hasFailures());
        }
        {
            // tag::search-scroll-example
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L)); // <1>

            SearchRequest searchRequest = new SearchRequest("posts"); // <2>
            searchRequest.source(new SearchSourceBuilder().query(matchQuery("title", "Elasticsearch")));
            searchRequest.scroll(scroll); // <3>

            SearchResponse searchResponse = client.search(searchRequest); // <4>
            String scrollId = searchResponse.getScrollId(); // <5>

            SearchHit[] searchHits = searchResponse.getHits().getHits(); // <6>
            while (searchHits != null && searchHits.length > 0) { // <7>
                SearchScrollRequest scrollRequest = new SearchScrollRequest() // <8>
                        .scroll(scroll) // <9>
                        .scrollId(scrollId);  // <10>

                searchResponse = client.searchScroll(scrollRequest); // <11>
                scrollId = searchResponse.getScrollId();  // <12>
                searchHits = searchResponse.getHits().getHits(); // <13>
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); // <14>
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest);
            // end::search-scroll-example
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.scroll("60s");

            SearchResponse initialSearchResponse = client.search(searchRequest);
            String scrollId = initialSearchResponse.getScrollId();

            SearchScrollRequest scrollRequest = new SearchScrollRequest();
            scrollRequest.scrollId(scrollId);

            // tag::scroll-request-scroll
            scrollRequest.scroll(TimeValue.timeValueSeconds(60L)); // <1>
            scrollRequest.scroll("60s"); // <2>
            // end::scroll-request-scroll

            // tag::search-scroll-execute-sync
            SearchResponse searchResponse = client.searchScroll(scrollRequest);
            // end::search-scroll-execute-sync

            assertEquals(0, searchResponse.getFailedShards());
            assertEquals(3L, searchResponse.getHits().getTotalHits());

            // tag::search-scroll-execute-async
            client.searchScrollAsync(scrollRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::search-scroll-execute-async

            // tag::clear-scroll-request
            ClearScrollRequest request = new ClearScrollRequest(); // <1>
            request.addScrollId(scrollId); // <2>
            // end::clear-scroll-request

            // tag::clear-scroll-add-scroll-id
            request.addScrollId(scrollId);
            // end::clear-scroll-add-scroll-id

            List<String> scrollIds = Arrays.asList(scrollId);

            // tag::clear-scroll-add-scroll-ids
            request.setScrollIds(scrollIds);
            // end::clear-scroll-add-scroll-ids

            // tag::clear-scroll-execute
            ClearScrollResponse response = client.clearScroll(request);
            // end::clear-scroll-execute

            // tag::clear-scroll-response
            boolean success = response.isSucceeded(); // <1>
            int released = response.getNumFreed(); // <2>
            // end::clear-scroll-response
            assertTrue(success);
            assertThat(released, greaterThan(0));

            // tag::clear-scroll-execute-async
            client.clearScrollAsync(request, new ActionListener<ClearScrollResponse>() {
                @Override
                public void onResponse(ClearScrollResponse clearScrollResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::clear-scroll-execute-async
        }
    }
}
