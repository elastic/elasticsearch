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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntOpenHashMap;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.missing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 *
 */
public class MetaDataTests extends ElasticsearchIntegrationTest {

    /**
     * Making sure that if there are multiple aggregations, working on the same field, yet require different
     * value source type, they can all still work. It used to fail as we used to cache the ValueSource by the
     * field name. If the cached value source was of type "bytes" and another aggregation on the field required to see
     * it as "numeric", it didn't work. Now we cache the Value Sources by a custom key (field name + ValueSource type)
     * so there's no conflict there.
     */
    @Test
    public void meta_data_set_on_aggregation_result() throws Exception {

        createIndex("idx");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomInt(30)];
        IntIntMap values = new IntIntOpenHashMap();
        long missingValues = 0;
        for (int i = 0; i < builders.length; i++) {
            String name = "name_" + randomIntBetween(1, 10);
            if (rarely()) {
                missingValues++;
                builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                        .startObject()
                        .field("name", name)
                        .endObject());
            } else {
                int value = randomIntBetween(1, 10);
                values.put(value, values.getOrDefault(value, 0) + 1);
                builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                        .startObject()
                        .field("name", name)
                        .field("value", value)
                        .endObject());
            }
        }
        indexRandom(true, builders);
        ensureSearchable();

        final Map<String, Object> nestedMetaData = new HashMap<String, Object>() {{
            put("nested", "value");
        }};

        Map<String, Object> missingValueMetaData = new HashMap<String, Object>() {{
            put("key", "value");
            put("numeric", 1.2);
            put("bool", true);
            put("complex", nestedMetaData);
        }};

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(missing("missing_values").field("value").setMetaData(missingValueMetaData))
                .execute().actionGet();

        assertSearchResponse(response);

        Aggregations aggs = response.getAggregations();
        assertNotNull(aggs);

        Missing missing = aggs.get("missing_values");
        assertNotNull(missing);
        assertThat(missing.getDocCount(), equalTo(missingValues));

        Map<String, Object> returnedMetaData = missing.getMetaData();
        assertNotNull(returnedMetaData);
        assertEquals(4, returnedMetaData.size());
        assertEquals("value", returnedMetaData.get("key"));
        assertEquals(1.2, returnedMetaData.get("numeric"));
        assertEquals(true, returnedMetaData.get("bool"));

        Object nestedObject = returnedMetaData.get("complex");
        assertNotNull(nestedObject);

        Map<String, Object> nestedMap = (Map<String, Object>)nestedObject;
        assertEquals("value", nestedMap.get("nested"));
    }


}
