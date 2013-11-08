/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SimpleMultiSearchTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleMultiSearch() {
        client().prepareIndex("test", "type", "1").setSource("field", "xxx").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "yyy").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        MultiSearchResponse response = client().prepareMultiSearch()
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();

        assertThat(response.getResponses().length, equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getHits().totalHits(), equalTo(1l));
        assertThat(response.getResponses()[1].getResponse().getHits().totalHits(), equalTo(1l));
        assertThat(response.getResponses()[2].getResponse().getHits().totalHits(), equalTo(2l));

        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).id(), equalTo("2"));
    }
}
