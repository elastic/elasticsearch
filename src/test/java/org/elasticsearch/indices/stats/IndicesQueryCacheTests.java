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

package org.elasticsearch.indices.stats;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.hamcrest.Matchers.greaterThan;

public class IndicesQueryCacheTests extends ElasticsearchIntegrationTest {

    // One of the primary purposes of the query cache is to cache aggs results
    public void testCacheAggs() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index").setSettings(IndicesQueryCache.INDEX_CACHE_QUERY_ENABLED, true).get());
        indexRandom(true,
                client().prepareIndex("index", "type").setSource("f", 4),
                client().prepareIndex("index", "type").setSource("f", 6),
                client().prepareIndex("index", "type").setSource("f", 7));

        final SearchResponse r1 = client().prepareSearch("index").setSearchType(SearchType.COUNT)
            .addAggregation(histogram("histo").field("f").interval(2)).get();

        // The cached is actually used
        assertThat(client().admin().indices().prepareStats("index").setQueryCache(true).get().getTotal().getQueryCache().getMemorySizeInBytes(), greaterThan(0l));

        for (int i = 0; i < 10; ++i) {
            final SearchResponse r2 = client().prepareSearch("index").setSearchType(SearchType.COUNT)
                    .addAggregation(histogram("histo").field("f").interval(2)).get();
            Histogram h1 = r1.getAggregations().get("histo");
            Histogram h2 = r2.getAggregations().get("histo");
            final List<? extends Bucket> buckets1 = h1.getBuckets();
            final List<? extends Bucket> buckets2 = h2.getBuckets();
            assertEquals(buckets1.size(), buckets2.size());
            for (int j = 0; j < buckets1.size(); ++j) {
                final Bucket b1 = buckets1.get(j);
                final Bucket b2 = buckets2.get(j);
                assertEquals(b1.getKey(), b2.getKey());
                assertEquals(b1.getDocCount(), b2.getDocCount());
            }
        }
    }

}
