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

package org.elasticsearch.flt;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyLikeThisFieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyLikeThisQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class FuzzyLikeThisActionTests extends ElasticsearchIntegrationTest {

    @Test
    // See issue https://github.com/elasticsearch/elasticsearch/issues/3252
    public void testNumericField() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type", "int_value", "type=integer"));
        ensureGreen();
        client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("string_value", "lucene index").field("int_value", 1).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "type", "2")
                .setSource(jsonBuilder().startObject().field("string_value", "elasticsearch index").field("int_value", 42).endObject())
                .execute().actionGet();

        refresh();

        // flt query with no field -> OK
        SearchResponse searchResponse = client().prepareSearch().setQuery(fuzzyLikeThisQuery().likeText("index")).execute().actionGet();
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));

        // flt query with string fields
        searchResponse = client().prepareSearch().setQuery(fuzzyLikeThisQuery("string_value").likeText("index")).execute().actionGet();
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));

        // flt query with at least a numeric field -> fail by default
        assertThrows(client().prepareSearch().setQuery(fuzzyLikeThisQuery("string_value", "int_value").likeText("index")), SearchPhaseExecutionException.class);

        // flt query with at least a numeric field -> fail by command
        assertThrows(client().prepareSearch().setQuery(fuzzyLikeThisQuery("string_value", "int_value").likeText("index").failOnUnsupportedField(true)), SearchPhaseExecutionException.class);


        // flt query with at least a numeric field but fail_on_unsupported_field set to false
        searchResponse = client().prepareSearch().setQuery(fuzzyLikeThisQuery("string_value", "int_value").likeText("index").failOnUnsupportedField(false)).execute().actionGet();
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));

        // flt field query on a numeric field -> failure by default
        assertThrows(client().prepareSearch().setQuery(fuzzyLikeThisFieldQuery("int_value").likeText("42")), SearchPhaseExecutionException.class);

        // flt field query on a numeric field -> failure by command
        assertThrows(client().prepareSearch().setQuery(fuzzyLikeThisFieldQuery("int_value").likeText("42").failOnUnsupportedField(true)), SearchPhaseExecutionException.class);

        // flt field query on a numeric field but fail_on_unsupported_field set to false
        searchResponse = client().prepareSearch().setQuery(fuzzyLikeThisFieldQuery("int_value").likeText("42").failOnUnsupportedField(false)).execute().actionGet();
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0L));
    }

}
