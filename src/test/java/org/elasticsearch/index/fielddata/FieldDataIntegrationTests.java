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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.Arrays;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;

public class FieldDataIntegrationTests extends ElasticsearchIntegrationTest {
    
    public void testDeprecatedFieldDataCaches() throws Exception {
        for (String cache : Arrays.asList(IndexFieldDataService.FIELDDATA_CACHE_VALUE_RESIDENT, IndexFieldDataService.FIELDDATA_CACHE_VALUE_SOFT)) {
            String index = "test" + cache;
            assertAcked(prepareCreate(index)
                .setSettings(IndexFieldDataService.FIELDDATA_CACHE_KEY, cache)
                .get());
            indexRandom(true, client().prepareIndex(index, "type").setSource("f", "g"));
            String message;
            try {
                final SearchResponse response = client().prepareSearch(index).addAggregation(AggregationBuilders.terms("f_terms").field("f")).get();
                assertFailures(response);
                message = response.getShardFailures()[0].reason();
            } catch (Exception e) {
                // expected
                message = e.getMessage();
            }
            assertTrue(message, message.contains("cache type not supported"));
        }
    }

}
