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

package org.elasticsearch.indices.cache.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.greaterThan;

public class IndicesRequestCacheIT extends ESIntegTestCase {

    // One of the primary purposes of the query cache is to cache aggs results
    public void testCacheAggs() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index")
                .addMapping("type", "f", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED, true).get());
        indexRandom(true,
                client().prepareIndex("index", "type").setSource("f", "2014-03-10T00:00:00.000Z"),
                client().prepareIndex("index", "type").setSource("f", "2014-05-13T00:00:00.000Z"));
        ensureSearchable("index");

        // This is not a random example: serialization with time zones writes shared strings
        // which used to not work well with the query cache because of the handles stream output
        // see #9500
        final SearchResponse r1 = client().prepareSearch("index").setSize(0).setSearchType(SearchType.QUERY_THEN_FETCH)
            .addAggregation(dateHistogram("histo").field("f").timeZone("+01:00").minDocCount(0).interval(DateHistogramInterval.MONTH)).get();
        assertSearchResponse(r1);

        // The cached is actually used
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(), greaterThan(0l));

        for (int i = 0; i < 10; ++i) {
            final SearchResponse r2 = client().prepareSearch("index").setSize(0).setSearchType(SearchType.QUERY_THEN_FETCH)
                    .addAggregation(dateHistogram("histo").field("f").timeZone("+01:00").minDocCount(0).interval(DateHistogramInterval.MONTH)).get();
            assertSearchResponse(r2);
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
