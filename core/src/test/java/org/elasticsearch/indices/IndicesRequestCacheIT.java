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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

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
        assertAcked(client().admin().indices().prepareCreate("index").addMapping("type", "s", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true,
                        IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5,
                        IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        indexRandom(true, client().prepareIndex("index", "type", "1").setRouting("1").setSource("s", "2016-03-19"),
                client().prepareIndex("index", "type", "2").setRouting("1").setSource("s", "2016-03-20"),
                client().prepareIndex("index", "type", "3").setRouting("1").setSource("s", "2016-03-21"),
                client().prepareIndex("index", "type", "4").setRouting("2").setSource("s", "2016-03-22"),
                client().prepareIndex("index", "type", "5").setRouting("2").setSource("s", "2016-03-23"),
                client().prepareIndex("index", "type", "6").setRouting("2").setSource("s", "2016-03-24"),
                client().prepareIndex("index", "type", "7").setRouting("3").setSource("s", "2016-03-25"),
                client().prepareIndex("index", "type", "8").setRouting("3").setSource("s", "2016-03-26"),
                client().prepareIndex("index", "type", "9").setRouting("3").setSource("s", "2016-03-27"));
        ensureSearchable("index");

        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r1 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-25")).get();
        assertSearchResponse(r1);
        assertThat(r1.getHits().getTotalHits(), equalTo(7L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(5L));

        final SearchResponse r2 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-20").lte("2016-03-26")).get();
        assertSearchResponse(r2);
        assertThat(r2.getHits().getTotalHits(), equalTo(7L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(3L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(7L));

        final SearchResponse r3 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-21").lte("2016-03-27")).get();
        assertSearchResponse(r3);
        assertThat(r3.getHits().getTotalHits(), equalTo(7L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(6L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(9L));
    }

    public void testQueryRewriteMissingValues() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index").addMapping("type", "s", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true, IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                        1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        indexRandom(true, client().prepareIndex("index", "type", "1").setSource("s", "2016-03-19"),
                client().prepareIndex("index", "type", "2").setSource("s", "2016-03-20"),
                client().prepareIndex("index", "type", "3").setSource("s", "2016-03-21"),
                client().prepareIndex("index", "type", "4").setSource("s", "2016-03-22"),
                client().prepareIndex("index", "type", "5").setSource("s", "2016-03-23"),
                client().prepareIndex("index", "type", "6").setSource("s", "2016-03-24"),
                client().prepareIndex("index", "type", "7").setSource("other", "value"),
                client().prepareIndex("index", "type", "8").setSource("s", "2016-03-26"),
                client().prepareIndex("index", "type", "9").setSource("s", "2016-03-27"));
        ensureSearchable("index");

        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r1 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-28")).get();
        assertSearchResponse(r1);
        assertThat(r1.getHits().getTotalHits(), equalTo(8L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));

        final SearchResponse r2 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-28")).get();
        assertSearchResponse(r2);
        assertThat(r2.getHits().getTotalHits(), equalTo(8L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(1L));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));

        final SearchResponse r3 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-28")).get();
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

    public void testQueryRewriteDatesWithNow() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index-1").addMapping("type", "d", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true, IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                        1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        assertAcked(client().admin().indices().prepareCreate("index-2").addMapping("type", "d", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true, IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                        1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        assertAcked(client().admin().indices().prepareCreate("index-3").addMapping("type", "d", "type=date")
                .setSettings(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true, IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                        1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .get());
        DateTime now = new DateTime(ISOChronology.getInstanceUTC());
        indexRandom(true, client().prepareIndex("index-1", "type", "1").setSource("d", now),
                client().prepareIndex("index-1", "type", "2").setSource("d", now.minusDays(1)),
                client().prepareIndex("index-1", "type", "3").setSource("d", now.minusDays(2)),
                client().prepareIndex("index-2", "type", "4").setSource("d", now.minusDays(3)),
                client().prepareIndex("index-2", "type", "5").setSource("d", now.minusDays(4)),
                client().prepareIndex("index-2", "type", "6").setSource("d", now.minusDays(5)),
                client().prepareIndex("index-3", "type", "7").setSource("d", now.minusDays(6)),
                client().prepareIndex("index-3", "type", "8").setSource("d", now.minusDays(7)),
                client().prepareIndex("index-3", "type", "9").setSource("d", now.minusDays(8)));
        ensureSearchable("index-1", "index-2", "index-3");

        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r1 = client().prepareSearch("index-*").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("d").gte("now-7d/d").lte("now")).get();
        assertSearchResponse(r1);
        assertThat(r1.getHits().getTotalHits(), equalTo(8L));
        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
        // Because the query will INTERSECT with the 3rd index it will not be
        // rewritten and will still contain `now` so won't be recorded as a
        // cache miss or cache hit since queries containing now can't be cached
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r2 = client().prepareSearch("index-*").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("d").gte("now-7d/d").lte("now")).get();
        assertSearchResponse(r2);
        assertThat(r2.getHits().getTotalHits(), equalTo(8L));
        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(1L));
        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(1L));
        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));

        final SearchResponse r3 = client().prepareSearch("index-*").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("d").gte("now-7d/d").lte("now")).get();
        assertSearchResponse(r3);
        assertThat(r3.getHits().getTotalHits(), equalTo(8L));
        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(2L));
        assertThat(
                client().admin().indices().prepareStats("index-1").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(2L));
        assertThat(
                client().admin().indices().prepareStats("index-2").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(1L));
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(0L));
        assertThat(
                client().admin().indices().prepareStats("index-3").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(0L));
    }

}
