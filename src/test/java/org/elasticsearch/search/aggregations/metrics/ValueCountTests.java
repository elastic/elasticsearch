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
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class ValueCountTests extends ElasticsearchIntegrationTest {

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+2).value(i+3).endArray()
                    .endObject())
                    .execute().actionGet();
        }
        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        ensureSearchable();
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(0l));
    }

    @Test
    public void singleValuedField() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10l));
    }

    @Test
    public void singleValuedField_getProperty() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(count("count").field("value"))).execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10l));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        ValueCount valueCount = global.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10l));
        assertThat((ValueCount) global.getProperty("count"), equalTo(valueCount));
        assertThat((double) global.getProperty("count.value"), equalTo(10d));
        assertThat((double) valueCount.getProperty("value"), equalTo(10d));
    }

    @Test
    public void singleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10l));
    }

    @Test
    public void multiValuedField() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(20l));
    }

    @Test
    public void singleValuedScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").script("doc['value'].value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10l));
    }

    @Test
    public void multiValuedScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").script("doc['values'].values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(20l));
    }

    @Test
    public void singleValuedScriptWithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").script("doc[s].value").param("s", "value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10l));
    }

    @Test
    public void multiValuedScriptWithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").script("doc[s].values").param("s", "values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(20l));
    }

}