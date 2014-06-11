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
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;
import sun.security.util.BigInt;

import java.math.BigInteger;
import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

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

        Integer missingValueMetaData = 1;
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(missing("missing_values").field("value").setMetaData(missingValueMetaData))
                .execute().actionGet();

        assertSearchResponse(response);

        Aggregations aggs = response.getAggregations();

        Missing missing = aggs.get("missing_values");
        assertNotNull(missing);
        assertThat(missing.getDocCount(), equalTo(missingValues));
        assertNotNull(missing.getMetaData());
    }


}
