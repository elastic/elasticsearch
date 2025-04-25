/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notANumber;

public class RandomSamplerAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD_NAME = "value";
    private static final String RANDOM_NUMERIC_FIELD_NAME = "random_numeric";
    private static final String KEYWORD_FIELD_NAME = "keyword";
    private static final String KEYWORD_FIELD_VALUE = "foo";
    private static final long TRUE_MIN = 2L;
    private static final long TRUE_MAX = 1005L;

    public void testAggregationSampling() throws IOException {
        double[] avgs = new double[5];
        long[] counts = new long[5];
        AtomicInteger integer = new AtomicInteger();
        do {
            testCase(RandomSamplerAggregatorTests::writeTestDocs, (InternalRandomSampler result) -> {
                counts[integer.get()] = result.getDocCount();
                if (result.getDocCount() > 0) {
                    Avg agg = result.getAggregations().get("avg");
                    assertThat(Strings.toString(result), agg.getValue(), allOf(not(notANumber()), isFinite()));
                    avgs[integer.get()] = agg.getValue();
                }
            },
                new AggTestConfig(
                    new RandomSamplerAggregationBuilder("my_agg").subAggregation(AggregationBuilders.avg("avg").field(NUMERIC_FIELD_NAME))
                        .setProbability(0.25),
                    longField(NUMERIC_FIELD_NAME)
                )
            );
        } while (integer.incrementAndGet() < 5);
        long avgCount = LongStream.of(counts).sum() / integer.get();
        double avgAvg = DoubleStream.of(avgs).sum() / integer.get();
        assertThat(avgCount, allOf(greaterThanOrEqualTo(20L), lessThanOrEqualTo(70L)));
        assertThat(avgAvg, closeTo(1.5, 0.5));
    }

    public void testAggregationSampling_withScores() throws IOException {
        long[] counts = new long[5];
        AtomicInteger integer = new AtomicInteger();
        do {
            testCase(RandomSamplerAggregatorTests::writeTestDocs, (InternalRandomSampler result) -> {
                counts[integer.get()] = result.getDocCount();
                if (result.getDocCount() > 0) {
                    TopHits agg = result.getAggregations().get("top");
                    List<SearchHit> hits = Arrays.asList(agg.getHits().getHits());
                    assertThat(Strings.toString(result), hits, hasSize(1));
                    assertThat(Strings.toString(result), hits.get(0).getScore(), allOf(greaterThan(0.0f), lessThan(1.0f)));
                }
            },
                new AggTestConfig(
                    new RandomSamplerAggregationBuilder("my_agg").subAggregation(AggregationBuilders.topHits("top").size(1))
                        .setProbability(0.25),
                    longField(NUMERIC_FIELD_NAME)
                ).withQuery(
                    new BooleanQuery.Builder().add(
                        new TermQuery(new Term(KEYWORD_FIELD_NAME, KEYWORD_FIELD_VALUE)),
                        BooleanClause.Occur.SHOULD
                    ).build()
                )
            );
        } while (integer.incrementAndGet() < 5);
        long avgCount = LongStream.of(counts).sum() / integer.get();
        assertThat(avgCount, allOf(greaterThanOrEqualTo(20L), lessThanOrEqualTo(70L)));
    }

    public void testAggregationSamplingNestedAggsScaled() throws IOException {
        // in case 0 docs get sampled, which can rarely happen
        // in case the test index has many segments.
        // subaggs should be scaled along with upper level aggs
        // sampled doc count is NOT scaled, and thus should be lower
        testCase(RandomSamplerAggregatorTests::writeTestDocs, (InternalRandomSampler result) -> {
            long sampledDocCount = result.getDocCount();
            SingleBucketAggregation agg = result.getAggregations().get("filter_outer");
            long outerFilterDocCount = agg.getDocCount();
            SingleBucketAggregation innerAgg = agg.getAggregations().get("filter_inner");
            long innerFilterDocCount = innerAgg.getDocCount();
            if (sampledDocCount == 0) {
                // in case 0 docs get sampled, which can rarely happen
                // in case the test index has many segments.
                assertThat(sampledDocCount, equalTo(0L));
                assertThat(innerFilterDocCount, equalTo(0L));
                assertThat(outerFilterDocCount, equalTo(0L));
            } else {
                // subaggs should be scaled along with upper level aggs
                assertThat(outerFilterDocCount, equalTo(innerFilterDocCount));
                // sampled doc count is NOT scaled, and thus should be lower
                assertThat(outerFilterDocCount, greaterThan(sampledDocCount));
            }
        },
            new AggTestConfig(
                new RandomSamplerAggregationBuilder("my_agg").subAggregation(
                    AggregationBuilders.filter("filter_outer", QueryBuilders.termsQuery(KEYWORD_FIELD_NAME, KEYWORD_FIELD_VALUE))
                        .subAggregation(
                            AggregationBuilders.filter("filter_inner", QueryBuilders.termsQuery(KEYWORD_FIELD_NAME, KEYWORD_FIELD_VALUE))
                        )
                ).setProbability(0.25),
                longField(NUMERIC_FIELD_NAME),
                keywordField(KEYWORD_FIELD_NAME)
            )
        );
    }

    public void testAggregationSamplingOptimizedMinAndMax() throws IOException {
        testCase(RandomSamplerAggregatorTests::writeTestDocsWithTrueMinMax, (InternalRandomSampler result) -> {
            Min min = result.getAggregations().get("min");
            Max max = result.getAggregations().get("max");
            assertThat(min.value(), equalTo((double) TRUE_MIN));
            assertThat(max.value(), equalTo((double) TRUE_MAX));
        },
            new AggTestConfig(
                new RandomSamplerAggregationBuilder("my_agg").subAggregation(
                    AggregationBuilders.max("max").field(RANDOM_NUMERIC_FIELD_NAME)
                ).subAggregation(AggregationBuilders.min("min").field(RANDOM_NUMERIC_FIELD_NAME)).setProbability(0.25),
                longField(RANDOM_NUMERIC_FIELD_NAME)
            )
        );
    }

    private static void writeTestDocsWithTrueMinMax(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 75; i++) {
            w.addDocument(List.of(new LongPoint(RANDOM_NUMERIC_FIELD_NAME, randomLongBetween(3, 1000))));
        }
        w.addDocument(List.of(new LongPoint(RANDOM_NUMERIC_FIELD_NAME, TRUE_MIN)));
        w.addDocument(List.of(new LongPoint(RANDOM_NUMERIC_FIELD_NAME, TRUE_MAX)));
    }

    private static void writeTestDocs(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 75; i++) {
            w.addDocument(
                List.of(
                    new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 1),
                    new KeywordFieldMapper.KeywordField(
                        KEYWORD_FIELD_NAME,
                        new BytesRef(KEYWORD_FIELD_VALUE),
                        KeywordFieldMapper.Defaults.FIELD_TYPE
                    )
                )
            );
        }
        for (int i = 0; i < 75; i++) {
            w.addDocument(
                List.of(
                    new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 2),
                    new KeywordFieldMapper.KeywordField(
                        KEYWORD_FIELD_NAME,
                        new BytesRef(KEYWORD_FIELD_VALUE),
                        KeywordFieldMapper.Defaults.FIELD_TYPE
                    )
                )
            );
        }
    }

    public static Matcher<Double> isFinite() {
        return new IsFinite();
    }

    private static class IsFinite extends TypeSafeMatcher<Double> {
        @Override
        protected boolean matchesSafely(Double item) {
            return Double.isFinite(item);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a finite double value");
        }
    }

}
