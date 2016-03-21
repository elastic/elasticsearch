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

package org.elasticsearch.indices;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTimeZone;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class IndicesRequestCacheIT extends ESIntegTestCase {

    // One of the primary purposes of the query cache is to cache aggs results
    public void testCacheAggs() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index")
                .addMapping("type", "f", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true).get());
        indexRandom(true,
                client().prepareIndex("index", "type").setSource("f", "2014-03-10T00:00:00.000Z"),
                client().prepareIndex("index", "type").setSource("f", "2014-05-13T00:00:00.000Z"));
        ensureSearchable("index");

        // This is not a random example: serialization with time zones writes shared strings
        // which used to not work well with the query cache because of the handles stream output
        // see #9500
        final SearchResponse r1 = client().prepareSearch("index").setSize(0).setSearchType(SearchType.QUERY_THEN_FETCH)
                .addAggregation(dateHistogram("histo").field("f").timeZone(DateTimeZone.forID("+01:00")).minDocCount(0)
                        .dateHistogramInterval(DateHistogramInterval.MONTH))
                .get();
        assertSearchResponse(r1);

        // The cached is actually used
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache()
            .getMemorySizeInBytes(), greaterThan(0L));

        for (int i = 0; i < 10; ++i) {
            final SearchResponse r2 = client().prepareSearch("index").setSize(0)
                    .setSearchType(SearchType.QUERY_THEN_FETCH).addAggregation(dateHistogram("histo").field("f")
                            .timeZone(DateTimeZone.forID("+01:00")).minDocCount(0).dateHistogramInterval(DateHistogramInterval.MONTH))
                    .get();
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

    public void testQueryRewrite() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index").addMapping("type", "s", "type=text")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true,
                        IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5,
                        IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        indexRandom(true, client().prepareIndex("index", "type", "1").setRouting("1").setSource("s", "a"),
                client().prepareIndex("index", "type", "2").setRouting("1").setSource("s", "b"),
                client().prepareIndex("index", "type", "3").setRouting("1").setSource("s", "c"),
                client().prepareIndex("index", "type", "4").setRouting("2").setSource("s", "d"),
                client().prepareIndex("index", "type", "5").setRouting("2").setSource("s", "e"),
                client().prepareIndex("index", "type", "6").setRouting("2").setSource("s", "f"),
                client().prepareIndex("index", "type", "7").setRouting("3").setSource("s", "g"),
                client().prepareIndex("index", "type", "8").setRouting("3").setSource("s", "h"),
                client().prepareIndex("index", "type", "9").setRouting("3").setSource("s", "i"));
        ensureSearchable("index");

        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r1 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("a").lte("g")).get();
        assertSearchResponse(r1);
        assertThat(r1.getHits().getTotalHits(), equalTo(7L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(5L));

        final SearchResponse r2 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("b").lte("h")).get();
        assertSearchResponse(r2);
        assertThat(r2.getHits().getTotalHits(), equalTo(7L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(3L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(7L));

        final SearchResponse r3 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("c").lte("i")).get();
        assertSearchResponse(r3);
        assertThat(r3.getHits().getTotalHits(), equalTo(7L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(6L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(9L));
    }

    public void testQueryRewriteMissingValues() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index").addMapping("type", "s", "type=text")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true, IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                        1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        indexRandom(true, client().prepareIndex("index", "type", "1").setSource("s", "a"),
                client().prepareIndex("index", "type", "2").setSource("s", "b"),
                client().prepareIndex("index", "type", "3").setSource("s", "c"),
                client().prepareIndex("index", "type", "4").setSource("s", "d"),
                client().prepareIndex("index", "type", "5").setSource("s", "e"),
                client().prepareIndex("index", "type", "6").setSource("s", "f"),
                client().prepareIndex("index", "type", "7").setSource("other", "value"),
                client().prepareIndex("index", "type", "8").setSource("s", "h"),
                client().prepareIndex("index", "type", "9").setSource("s", "i"));
        ensureSearchable("index");

        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r1 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("a").lte("j")).get();
        assertSearchResponse(r1);
        assertThat(r1.getHits().getTotalHits(), equalTo(8L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));

        final SearchResponse r2 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("a").lte("j")).get();
        assertSearchResponse(r2);
        assertThat(r2.getHits().getTotalHits(), equalTo(8L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(1L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));

        final SearchResponse r3 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("a").lte("j")).get();
        assertSearchResponse(r3);
        assertThat(r3.getHits().getTotalHits(), equalTo(8L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(2L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
    }

    public void testQueryRewriteDates() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index").addMapping("type", "d", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true,
                        IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1,
                        IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        indexRandom(true, client().prepareIndex("index", "type", "1").setSource("d", "2014-01-01T00:00:00"),
                client().prepareIndex("index", "type", "2").setSource("d", "2014-02-01T00:00:00"),
                client().prepareIndex("index", "type", "3").setSource("d", "2014-03-01T00:00:00"),
                client().prepareIndex("index", "type", "4").setSource("d", "2014-04-01T00:00:00"),
                client().prepareIndex("index", "type", "5").setSource("d", "2014-05-01T00:00:00"),
                client().prepareIndex("index", "type", "6").setSource("d", "2014-06-01T00:00:00"),
                client().prepareIndex("index", "type", "7").setSource("d", "2014-07-01T00:00:00"),
                client().prepareIndex("index", "type", "8").setSource("d", "2014-08-01T00:00:00"),
                client().prepareIndex("index", "type", "9").setSource("d", "2014-09-01T00:00:00"));
        ensureSearchable("index");

        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r1 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("d").gte("2013-01-01T00:00:00").lte("now"))
                .get();
        assertSearchResponse(r1);
        assertThat(r1.getHits().getTotalHits(), equalTo(9L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));

        final SearchResponse r2 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("d").gte("2013-01-01T00:00:00").lte("now"))
                .get();
        assertSearchResponse(r2);
        assertThat(r2.getHits().getTotalHits(), equalTo(9L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(1L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));

        final SearchResponse r3 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("d").gte("2013-01-01T00:00:00").lte("now"))
                .get();
        assertSearchResponse(r3);
        assertThat(r3.getHits().getTotalHits(), equalTo(9L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(2L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
    }

}
