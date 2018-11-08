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
package org.elasticsearch.search.query;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class MatchQueryIT extends ESIntegTestCase {

     public void testJsonFields() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("headers")
                        .field("type", "json")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                .endObject()
           .endObject()
        .endObject();
        assertAcked(prepareCreate("test").addMapping("_doc", mapping));

        IndexRequestBuilder indexRequest = client().prepareIndex("test", "_doc", "1")
           .setSource(XContentFactory.jsonBuilder()
               .startObject()
                   .startObject("headers")
                       .field("content-type", "application/json")
                       .field("origin", "https://www.elastic.co")
                   .endObject()
           .endObject());
        indexRandom(true, false, indexRequest);

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchQuery("headers", "application/json"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(matchQuery("headers.content-type", "application/json text/plain"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(matchQuery("headers.origin", "application/json"))
            .get();
        assertHitCount(searchResponse, 0L);
    }
}
