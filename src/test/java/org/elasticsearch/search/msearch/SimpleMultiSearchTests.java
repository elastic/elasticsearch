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

package org.elasticsearch.search.msearch;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SimpleMultiSearchTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleMultiSearch() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type", "1").setSource("field", "xxx").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "yyy").execute().actionGet();
        refresh();
        MultiSearchResponse response = client().prepareMultiSearch()
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        
        for (MultiSearchResponse.Item item : response) {
           assertNoFailures(item.getResponse());
        }
        assertThat(response.getResponses().length, equalTo(3));
        assertHitCount(response.getResponses()[0].getResponse(), 1l);
        assertHitCount(response.getResponses()[1].getResponse(), 1l);
        assertHitCount(response.getResponses()[2].getResponse(), 2l);
        assertFirstHit(response.getResponses()[0].getResponse(), hasId("1"));
        assertFirstHit(response.getResponses()[1].getResponse(), hasId("2"));
    }
}
