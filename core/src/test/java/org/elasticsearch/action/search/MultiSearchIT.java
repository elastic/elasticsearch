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

import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
/**
 * MultiSearch IT
 */
public class MultiSearchIT extends ESSingleNodeTestCase {
    //Verifies that the MultiSearch took time is ge to any of the individual search took times
    public void testMultiSearchTookTimeAgainstSearchTookTime () throws Exception {
        createIndex("test", Settings.EMPTY, "test", "title", "type=text");
        client().prepareIndex("test", "test", "1").setSource("title", "foo bar baz").get();
        client().prepareIndex("test", "test", "2").setSource("title", "foo foo foo").get();
        client().prepareIndex("test", "test", "3").setSource("title", "bar baz bax").get();
        client().admin().indices().prepareRefresh("test").get();
        
        SearchRequest searchRequestAll = new SearchRequest().indices("test")
                .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        SearchRequest searchRequestBar = new SearchRequest().indices("test")
                .source(new SearchSourceBuilder().query(new MatchQueryBuilder("title", "bar")));

        MultiSearchRequest multiSeachRequest = new MultiSearchRequest();
        multiSeachRequest.add(searchRequestAll);
        multiSeachRequest.add(searchRequestBar);

        MultiSearchResponse multiSearchResponse = client().multiSearch(multiSeachRequest).get();
        
        long maxSearchResponseTookTime = 0;
        for (Item response : multiSearchResponse.getResponses()) {
            maxSearchResponseTookTime = Math.max(maxSearchResponseTookTime,
                    response.getResponse().getTookInMillis());
        }
        
        long multiSearchResponseTime = multiSearchResponse.getTookInMillis();
        
        assertThat(multiSearchResponseTime, greaterThanOrEqualTo(maxSearchResponseTookTime));
    }
}
