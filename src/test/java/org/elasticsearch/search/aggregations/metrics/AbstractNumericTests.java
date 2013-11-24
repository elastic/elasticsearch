/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public abstract class AbstractNumericTests extends ElasticsearchIntegrationTest {
    
    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();

        for (int i = 0; i < 10; i++) { // TODO randomize the size and the params in here?
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+2).value(i+3).endArray()
                    .endObject()));
        }
        indexRandom(true, builders);

        // creating an index to test the empty buckets functionality. The way it works is by indexing
        // two docs {value: 0} and {value : 2}, then building a histogram agg with interval 1 and with empty
        // buckets computed.. the empty bucket is the one associated with key "1". then each test will have
        // to check that this bucket exists with the appropriate sub aggregations.
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        builders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        indexRandom(true, builders);

    }

    public abstract void testEmptyAggregation() throws Exception;

    public abstract void testUnmapped() throws Exception;

    public abstract void testSingleValuedField() throws Exception;

    public abstract void testSingleValuedField_PartiallyUnmapped() throws Exception;

    public abstract void testSingleValuedField_WithValueScript() throws Exception;

    public abstract void testSingleValuedField_WithValueScript_WithParams() throws Exception;

    public abstract void testMultiValuedField() throws Exception;

    public abstract void testMultiValuedField_WithValueScript() throws Exception;

    public abstract void testMultiValuedField_WithValueScript_WithParams() throws Exception;

    public abstract void testScript_SingleValued() throws Exception;

    public abstract void testScript_SingleValued_WithParams() throws Exception;

    public abstract void testScript_ExplicitSingleValued_WithParams() throws Exception;

    public abstract void testScript_MultiValued() throws Exception;

    public abstract void testScript_ExplicitMultiValued() throws Exception;

    public abstract void testScript_MultiValued_WithParams() throws Exception;


}