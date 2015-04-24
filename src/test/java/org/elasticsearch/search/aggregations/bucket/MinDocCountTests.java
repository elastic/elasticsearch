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

package org.elasticsearch.search.aggregations.bucket;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;


@ElasticsearchIntegrationTest.SuiteScopeTest
public class MinDocCountTests extends AbstractTermsTests {

    private static final QueryBuilder QUERY = QueryBuilders.termQuery("match", true);

    private static int cardinality;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");

        cardinality = randomIntBetween(8, 30);
        final List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        final Set<String> stringTerms = new HashSet<>();
        final LongSet longTerms = new LongOpenHashSet();
        final Set<String> dateTerms = new HashSet<>();
        for (int i = 0; i < cardinality; ++i) {
            String stringTerm;
            do {
                stringTerm = RandomStrings.randomAsciiOfLength(getRandom(), 8);
            } while (!stringTerms.add(stringTerm));
            long longTerm;
            do {
                longTerm = randomInt(cardinality * 2);
            } while (!longTerms.add(longTerm));
            double doubleTerm = longTerm * Math.PI;
            String dateTerm = DateTimeFormat.forPattern("yyyy-MM-dd").print(new DateTime(2014, 1, ((int) longTerm % 20) + 1, 0, 0));
            final int frequency = randomBoolean() ? 1 : randomIntBetween(2, 20);
            for (int j = 0; j < frequency; ++j) {
                indexRequests.add(client().prepareIndex("idx", "type").setSource(jsonBuilder()
                        .startObject()
                        .field("s", stringTerm)
                        .field("l", longTerm)
                        .field("d", doubleTerm)
                        .field("date", dateTerm)
                        .field("match", randomBoolean())
                        .endObject()));
            }
        }
        cardinality = stringTerms.size();

        indexRandom(true, indexRequests);
        ensureSearchable();
    }

    private enum Script {
        NO {
            @Override
            TermsBuilder apply(TermsBuilder builder, String field) {
                return builder.field(field);
            }
        },
        YES {
            @Override
            TermsBuilder apply(TermsBuilder builder, String field) {
                return builder.script("doc['" + field + "'].values");
            }
        };
        abstract TermsBuilder apply(TermsBuilder builder, String field);
    }

    // check that terms2 is a subset of terms1
    private void assertSubset(Terms terms1, Terms terms2, long minDocCount, int size, String include) {
        final Matcher matcher = include == null ? null : Pattern.compile(include).matcher("");;
        final Iterator<Terms.Bucket> it1 = terms1.getBuckets().iterator();
        final Iterator<Terms.Bucket> it2 = terms2.getBuckets().iterator();
        int size2 = 0;
        while (it1.hasNext()) {
            final Terms.Bucket bucket1 = it1.next();
            if (bucket1.getDocCount() >= minDocCount && (matcher == null || matcher.reset(bucket1.getKeyAsString()).matches())) {
                if (size2++ == size) {
                    break;
                }
                assertTrue(it2.hasNext());
                final Terms.Bucket bucket2 = it2.next();
                assertEquals(bucket1.getDocCount(), bucket2.getDocCount());
            }
        }
        assertFalse(it2.hasNext());
    }

    private void assertSubset(Histogram histo1, Histogram histo2, long minDocCount) {
        final Iterator<? extends Histogram.Bucket> it2 = histo2.getBuckets().iterator();
        for (Histogram.Bucket b1 : histo1.getBuckets()) {
            if (b1.getDocCount() >= minDocCount) {
                final Histogram.Bucket b2 = it2.next();
                assertEquals(b1.getKey(), b2.getKey());
                assertEquals(b1.getDocCount(), b2.getDocCount());
            }
        }
    }

    public void testStringTermAsc() throws Exception {
        testMinDocCountOnTerms("s", Script.NO, Terms.Order.term(true));
    }

    public void testStringScriptTermAsc() throws Exception {
        testMinDocCountOnTerms("s", Script.YES, Terms.Order.term(true));
    }

    public void testStringTermDesc() throws Exception {
        testMinDocCountOnTerms("s", Script.NO, Terms.Order.term(false));
    }

    public void testStringScriptTermDesc() throws Exception {
        testMinDocCountOnTerms("s", Script.YES, Terms.Order.term(false));
    }

    public void testStringCountAsc() throws Exception {
        testMinDocCountOnTerms("s", Script.NO, Terms.Order.count(true));
    }

    public void testStringScriptCountAsc() throws Exception {
        testMinDocCountOnTerms("s", Script.YES, Terms.Order.count(true));
    }

    public void testStringCountDesc() throws Exception {
        testMinDocCountOnTerms("s", Script.NO, Terms.Order.count(false));
    }

    public void testStringScriptCountDesc() throws Exception {
        testMinDocCountOnTerms("s", Script.YES, Terms.Order.count(false));
    }

    public void testStringCountAscWithInclude() throws Exception {
        testMinDocCountOnTerms("s", Script.NO, Terms.Order.count(true), ".*a.*", true);
    }

    public void testStringScriptCountAscWithInclude() throws Exception {
        testMinDocCountOnTerms("s", Script.YES, Terms.Order.count(true), ".*a.*", true);
    }

    public void testStringCountDescWithInclude() throws Exception {
        testMinDocCountOnTerms("s", Script.NO, Terms.Order.count(false), ".*a.*", true);
    }

    public void testStringScriptCountDescWithInclude() throws Exception {
        testMinDocCountOnTerms("s", Script.YES, Terms.Order.count(false), ".*a.*", true);
    }

    public void testLongTermAsc() throws Exception {
        testMinDocCountOnTerms("l", Script.NO, Terms.Order.term(true));
    }

    public void testLongScriptTermAsc() throws Exception {
        testMinDocCountOnTerms("l", Script.YES, Terms.Order.term(true));
    }

    public void testLongTermDesc() throws Exception {
        testMinDocCountOnTerms("l", Script.NO, Terms.Order.term(false));
    }

    public void testLongScriptTermDesc() throws Exception {
        testMinDocCountOnTerms("l", Script.YES, Terms.Order.term(false));
    }

    public void testLongCountAsc() throws Exception {
        testMinDocCountOnTerms("l", Script.NO, Terms.Order.count(true));
    }

    public void testLongScriptCountAsc() throws Exception {
        testMinDocCountOnTerms("l", Script.YES, Terms.Order.count(true));
    }

    public void testLongCountDesc() throws Exception {
        testMinDocCountOnTerms("l", Script.NO, Terms.Order.count(false));
    }

    public void testLongScriptCountDesc() throws Exception {
        testMinDocCountOnTerms("l", Script.YES, Terms.Order.count(false));
    }

    public void testDoubleTermAsc() throws Exception {
        testMinDocCountOnTerms("d", Script.NO, Terms.Order.term(true));
    }

    public void testDoubleScriptTermAsc() throws Exception {
        testMinDocCountOnTerms("d", Script.YES, Terms.Order.term(true));
    }

    public void testDoubleTermDesc() throws Exception {
        testMinDocCountOnTerms("d", Script.NO, Terms.Order.term(false));
    }

    public void testDoubleScriptTermDesc() throws Exception {
        testMinDocCountOnTerms("d", Script.YES, Terms.Order.term(false));
    }

    public void testDoubleCountAsc() throws Exception {
        testMinDocCountOnTerms("d", Script.NO, Terms.Order.count(true));
    }

    public void testDoubleScriptCountAsc() throws Exception {
        testMinDocCountOnTerms("d", Script.YES, Terms.Order.count(true));
    }

    public void testDoubleCountDesc() throws Exception {
        testMinDocCountOnTerms("d", Script.NO, Terms.Order.count(false));
    }

    public void testDoubleScriptCountDesc() throws Exception {
        testMinDocCountOnTerms("d", Script.YES, Terms.Order.count(false));
    }

    private void testMinDocCountOnTerms(String field, Script script, Terms.Order order) throws Exception {
        testMinDocCountOnTerms(field, script, order, null, true);
    }

    private void testMinDocCountOnTerms(String field, Script script, Terms.Order order, String include, boolean retryOnFailure) throws Exception {
        // all terms
        final SearchResponse allTermsResponse = client().prepareSearch("idx").setTypes("type")
                .setSize(0)
                .setQuery(QUERY)
                .addAggregation(script.apply(terms("terms"), field)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .executionHint(randomExecutionHint())
                        .order(order)
                        .size(cardinality + randomInt(10))
                        .minDocCount(0))
                .execute().actionGet();
        assertAllSuccessful(allTermsResponse);

        final Terms allTerms = allTermsResponse.getAggregations().get("terms");
        assertEquals(cardinality, allTerms.getBuckets().size());

        for (long minDocCount = 0; minDocCount < 20; ++minDocCount) {
            final int size = randomIntBetween(1, cardinality + 2);
            final SearchRequest request = client().prepareSearch("idx").setTypes("type")
                    .setSize(0)
                    .setQuery(QUERY)
                    .addAggregation(script.apply(terms("terms"), field)
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                            .executionHint(randomExecutionHint())
                            .order(order)
                            .size(size)
                            .include(include)
                            .shardSize(cardinality + randomInt(10))
                            .minDocCount(minDocCount)).request();
            final SearchResponse response = client().search(request).get();
            try {
                assertAllSuccessful(response);
                assertSubset(allTerms, (Terms) response.getAggregations().get("terms"), minDocCount, size, include);
            } catch (AssertionError ae) {
                if (!retryOnFailure) {
                    throw ae;
                }
                logger.info("test failed. trying to see if it recovers after 1m.", ae);
                try {
                    Thread.sleep(60000);
                    logger.debug("1m passed. retrying.");
                    testMinDocCountOnTerms(field, script, order, include, false);
                } catch (Throwable secondFailure) {
                    logger.error("exception on retry (will re-throw the original in a sec)", secondFailure);
                } finally {
                    throw ae;
                }
            }
        }

    }

    public void testHistogramCountAsc() throws Exception {
        testMinDocCountOnHistogram(Histogram.Order.COUNT_ASC);
    }

    public void testHistogramCountDesc() throws Exception {
        testMinDocCountOnHistogram(Histogram.Order.COUNT_DESC);
    }

    public void testHistogramKeyAsc() throws Exception {
        testMinDocCountOnHistogram(Histogram.Order.KEY_ASC);
    }

    public void testHistogramKeyDesc() throws Exception {
        testMinDocCountOnHistogram(Histogram.Order.KEY_DESC);
    }

    public void testDateHistogramCountAsc() throws Exception {
        testMinDocCountOnDateHistogram(Histogram.Order.COUNT_ASC);
    }

    public void testDateHistogramCountDesc() throws Exception {
        testMinDocCountOnDateHistogram(Histogram.Order.COUNT_DESC);
    }

    public void testDateHistogramKeyAsc() throws Exception {
        testMinDocCountOnDateHistogram(Histogram.Order.KEY_ASC);
    }

    public void testDateHistogramKeyDesc() throws Exception {
        testMinDocCountOnDateHistogram(Histogram.Order.KEY_DESC);
    }

    private void testMinDocCountOnHistogram(Histogram.Order order) throws Exception {
        final int interval = randomIntBetween(1, 3);
        final SearchResponse allResponse = client().prepareSearch("idx").setTypes("type")
                .setSize(0)
                .setQuery(QUERY)
                .addAggregation(histogram("histo").field("d").interval(interval).order(order).minDocCount(0))
                .execute().actionGet();

        final Histogram allHisto = allResponse.getAggregations().get("histo");

        for (long minDocCount = 0; minDocCount < 50; ++minDocCount) {
            final SearchResponse response = client().prepareSearch("idx").setTypes("type")
                    .setSize(0)
                    .setQuery(QUERY)
                    .addAggregation(histogram("histo").field("d").interval(interval).order(order).minDocCount(minDocCount))
                    .execute().actionGet();
            assertSubset(allHisto, (Histogram) response.getAggregations().get("histo"), minDocCount);
        }

    }

    private void testMinDocCountOnDateHistogram(Histogram.Order order) throws Exception {
        final int interval = randomIntBetween(1, 3);
        final SearchResponse allResponse = client().prepareSearch("idx").setTypes("type")
                .setSize(0)
                .setQuery(QUERY)
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.DAY).order(order).minDocCount(0))
                .execute().actionGet();

        final Histogram allHisto = allResponse.getAggregations().get("histo");

        for (long minDocCount = 0; minDocCount < 50; ++minDocCount) {
            final SearchResponse response = client().prepareSearch("idx").setTypes("type")
                    .setSize(0)
                    .setQuery(QUERY)
                    .addAggregation(dateHistogram("histo").field("date").interval(DateHistogramInterval.DAY).order(order).minDocCount(minDocCount))
                    .execute().actionGet();
            assertSubset(allHisto, (Histogram) response.getAggregations().get("histo"), minDocCount);
        }

    }

}
