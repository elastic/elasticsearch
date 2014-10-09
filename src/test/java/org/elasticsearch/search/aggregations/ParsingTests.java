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

package org.elasticsearch.search.aggregations;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParsingTests extends ElasticsearchIntegrationTest {

    @Test(expected=SearchPhaseExecutionException.class)
    public void testTwoTypes() throws Exception {
        createIndex("idx");
        ensureGreen();
        client().prepareSearch("idx").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject("in_stock")
                    .startObject("filter")
                        .startObject("range")
                            .startObject("stock")
                                .field("gt", 0)
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("terms")
                        .field("field", "stock")
                    .endObject()
                .endObject()
            .endObject()).execute().actionGet();
    }

    @Test(expected=SearchPhaseExecutionException.class)
    public void testTwoAggs() throws Exception {
        createIndex("idx");
        ensureGreen();
        client().prepareSearch("idx").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject("by_date")
                    .startObject("date_histogram")
                        .field("field", "timestamp")
                        .field("interval", "month")
                    .endObject()
                    .startObject("aggs")
                        .startObject("tag_count")
                            .startObject("cardinality")
                                .field("field", "tag")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("aggs") // 2nd "aggs": illegal
                        .startObject("tag_count2")
                            .startObject("cardinality")
                                .field("field", "tag")
                            .endObject()
                        .endObject()
                    .endObject()
            .endObject()).execute().actionGet();
    }

    @Test(expected=SearchPhaseExecutionException.class)
    public void testInvalidAggregationName() throws Exception {

        Matcher matcher = Pattern.compile("[^\\[\\]>]+").matcher("");
        String name;
        SecureRandom rand = new SecureRandom();
        int len = randomIntBetween(1, 5);
        char[] word = new char[len];
        while(true) {
            for (int i = 0; i < word.length; i++) {
                word[i] = (char) rand.nextInt(127);
            }
            name = String.valueOf(word);
            if (!matcher.reset(name).matches()) {
                break;
            }
        }

        createIndex("idx");
        ensureGreen();
        client().prepareSearch("idx").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject(name)
                    .startObject("filter")
                        .startObject("range")
                            .startObject("stock")
                                .field("gt", 0)
                            .endObject()
                        .endObject()
                    .endObject()
            .endObject()).execute().actionGet();
    }

    @Test(expected=SearchPhaseExecutionException.class)
    public void testSameAggregationName() throws Exception {
        createIndex("idx");
        ensureGreen();
        final String name = RandomStrings.randomAsciiOfLength(getRandom(), 10);
        client().prepareSearch("idx").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject(name)
                    .startObject("terms")
                        .field("field", "a")
                    .endObject()
                .endObject()
                .startObject(name)
                    .startObject("terms")
                        .field("field", "b")
                    .endObject()
                .endObject()
            .endObject()).execute().actionGet();
    }

    @Test(expected=SearchPhaseExecutionException.class)
    public void testMissingName() throws Exception {
        createIndex("idx");
        ensureGreen();
        client().prepareSearch("idx").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject("by_date")
                    .startObject("date_histogram")
                        .field("field", "timestamp")
                        .field("interval", "month")
                    .endObject()
                    .startObject("aggs")
                        // the aggregation name is missing
                        //.startObject("tag_count")
                            .startObject("cardinality")
                                .field("field", "tag")
                            .endObject()
                        //.endObject()
                    .endObject()
            .endObject()).execute().actionGet();
    }

    @Test(expected=SearchPhaseExecutionException.class)
    public void testMissingType() throws Exception {
        createIndex("idx");
        ensureGreen();
        client().prepareSearch("idx").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject("by_date")
                    .startObject("date_histogram")
                        .field("field", "timestamp")
                        .field("interval", "month")
                    .endObject()
                    .startObject("aggs")
                        .startObject("tag_count")
                            // the aggregation type is missing
                            //.startObject("cardinality")
                                .field("field", "tag")
                            //.endObject()
                        .endObject()
                    .endObject()
            .endObject()).execute().actionGet();
    }

}
