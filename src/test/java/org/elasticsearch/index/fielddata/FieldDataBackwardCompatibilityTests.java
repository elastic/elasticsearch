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

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;

import java.util.Arrays;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class FieldDataBackwardCompatibilityTests extends ElasticsearchBackwardsCompatIntegrationTest {

    public void testDeprecatedFieldDataCaches() throws Exception {
        for (String cache : Arrays.asList(IndexFieldDataService.FIELDDATA_CACHE_VALUE_RESIDENT, IndexFieldDataService.FIELDDATA_CACHE_VALUE_SOFT)) {
            String index = "test" + cache;
            assertAcked(prepareCreate(index)
                .setSettings(IndexFieldDataService.FIELDDATA_CACHE_KEY, cache)
                .get());
            ensureYellow(index);
            indexRandom(true, client().prepareIndex(index, "type").setSource("f", "g"));
            String message = null;
            try {
                final SearchResponse response = client().prepareSearch(index).addAggregation(AggregationBuilders.terms("f_terms").field("f")).get();
                if (response.getShardFailures().length > 0) {
                    message = response.getShardFailures()[0].reason();
                }
            } catch (Exception e) {
                // expected
                message = e.getMessage();
            }
            if (compatibilityVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
                assertNotNull(message);
                assertTrue(message, message.contains("cache type not supported"));
            } else {
                assertNull(message);
            }
        }
    }

}
