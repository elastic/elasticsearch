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
package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class HighLevelRestClientFilterPathIT extends ESRestHighLevelClientTestCase {

    private static final String SAMPLE_DOCUMENT = "{\"name\":{\"first name\":\"Steve\",\"last name\":\"Jobs\"}}";
    private static final String FILTER_PATH_PARAM = "filter_path";
    private static final String FILTER_PATH_PARAM_VALUE = "-hits.hits._index,-hits.hits._type,-hits.hits.matched_queries";

    public void testUsingFilterPathWithHitsIndexResultsIntoEmptyIndexNameInInnerHit() throws IOException {
        Request doc = new Request(HttpPut.METHOD_NAME, "/company_one/_doc/1");
        doc.setJsonEntity(SAMPLE_DOCUMENT);
        client().performRequest(doc);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addParameter(FILTER_PATH_PARAM, FILTER_PATH_PARAM_VALUE)
            .build();

        SearchRequest searchRequest = new SearchRequest("company_one");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync, requestOptions);

        assertThat(searchResponse.status().getStatus(), equalTo(200));
        assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        assertNull(searchResponse.getHits().getHits()[0].getIndex());
        assertEquals(SAMPLE_DOCUMENT, searchResponse.getHits().getHits()[0].getSourceAsString());
    }

    public void testNotUsingFilterPathResultsIntoIndexNameInInnerHit() throws IOException {
        Request doc = new Request(HttpPut.METHOD_NAME, "/company_two/_doc/1");
        doc.setJsonEntity(SAMPLE_DOCUMENT);
        client().performRequest(doc);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        RequestOptions requestOptions = RequestOptions.DEFAULT;
        SearchRequest searchRequest = new SearchRequest("company_two");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync, requestOptions);

        assertThat(searchResponse.status().getStatus(), equalTo(200));
        assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        assertEquals("company_two", searchResponse.getHits().getHits()[0].getIndex());
        assertEquals(SAMPLE_DOCUMENT, searchResponse.getHits().getHits()[0].getSourceAsString());
    }

}
