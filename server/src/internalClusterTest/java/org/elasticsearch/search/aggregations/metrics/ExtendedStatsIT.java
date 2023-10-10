/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats.Bounds;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.missing;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ExtendedStatsIT extends AbstractNumericTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(AggregationTestScriptsPlugin.class);
    }

    private static double stdDev(int... vals) {
        return Math.sqrt(variance(vals));
    }

    private static double stdDevPopulation(int... vals) {
        return Math.sqrt(variancePopulation(vals));
    }

    private static double stdDevSampling(int... vals) {
        return Math.sqrt(varianceSampling(vals));
    }

    private static double variance(int... vals) {
        return variancePopulation(vals);
    }

    private static double variancePopulation(int... vals) {
        double sum = 0;
        double sumOfSqrs = 0;
        for (int val : vals) {
            sum += val;
            sumOfSqrs += val * val;
        }
        double variance = (sumOfSqrs - ((sum * sum) / vals.length)) / vals.length;
        return variance < 0 ? 0 : variance;
    }

    private static double varianceSampling(int... vals) {
        double sum = 0;
        double sumOfSqrs = 0;
        for (int val : vals) {
            sum += val;
            sumOfSqrs += val * val;
        }
        double variance = (sumOfSqrs - ((sum * sum) / vals.length)) / (vals.length - 1);
        return variance < 0 ? 0 : variance;
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field("value").interval(1L).minDocCount(0).subAggregation(extendedStats("stats").field("value"))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        ExtendedStats stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getSumOfSquares(), equalTo(0.0));
        assertThat(stats.getCount(), equalTo(0L));
        assertThat(stats.getSum(), equalTo(0.0));
        assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(Double.isNaN(stats.getStdDeviation()), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationPopulation()), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationSampling()), is(true));
        assertThat(Double.isNaN(stats.getAvg()), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER_POPULATION)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER_POPULATION)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER_SAMPLING)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER_SAMPLING)), is(true));
    }

    @Override
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").field("value"))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo(Double.NaN));
        assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(stats.getSum(), equalTo(0.0));
        assertThat(stats.getCount(), equalTo(0L));
        assertThat(stats.getSumOfSquares(), equalTo(0.0));
        assertThat(stats.getVariance(), equalTo(Double.NaN));
        assertThat(stats.getVariancePopulation(), equalTo(Double.NaN));
        assertThat(stats.getVarianceSampling(), equalTo(Double.NaN));
        assertThat(stats.getStdDeviation(), equalTo(Double.NaN));
        assertThat(stats.getStdDeviationPopulation(), equalTo(Double.NaN));
        assertThat(stats.getStdDeviationSampling(), equalTo(Double.NaN));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER_POPULATION)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER_POPULATION)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER_SAMPLING)), is(true));
        assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER_SAMPLING)), is(true));
    }

    public void testPartiallyUnmapped() {
        double sigma = randomDouble() * 5;
        ExtendedStats s1 = client().prepareSearch("idx")
            .addAggregation(extendedStats("stats").field("value").sigma(sigma))
            .get()
            .getAggregations()
            .get("stats");
        ExtendedStats s2 = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(extendedStats("stats").field("value").sigma(sigma))
            .get()
            .getAggregations()
            .get("stats");
        assertEquals(s1.getAvg(), s2.getAvg(), 1e-10);
        assertEquals(s1.getCount(), s2.getCount());
        assertEquals(s1.getMin(), s2.getMin(), 0d);
        assertEquals(s1.getMax(), s2.getMax(), 0d);
        assertEquals(s1.getStdDeviation(), s2.getStdDeviation(), 1e-10);
        assertEquals(s1.getStdDeviationPopulation(), s2.getStdDeviationPopulation(), 1e-10);
        assertEquals(s1.getStdDeviationSampling(), s2.getStdDeviationSampling(), 1e-10);
        assertEquals(s1.getSumOfSquares(), s2.getSumOfSquares(), 1e-10);
        assertEquals(s1.getStdDeviationBound(Bounds.LOWER), s2.getStdDeviationBound(Bounds.LOWER), 1e-10);
        assertEquals(s1.getStdDeviationBound(Bounds.UPPER), s2.getStdDeviationBound(Bounds.UPPER), 1e-10);
        assertEquals(s1.getStdDeviationBound(Bounds.LOWER_POPULATION), s2.getStdDeviationBound(Bounds.LOWER_POPULATION), 1e-10);
        assertEquals(s1.getStdDeviationBound(Bounds.UPPER_POPULATION), s2.getStdDeviationBound(Bounds.UPPER_POPULATION), 1e-10);
        assertEquals(s1.getStdDeviationBound(Bounds.LOWER_SAMPLING), s2.getStdDeviationBound(Bounds.LOWER_SAMPLING), 1e-10);
        assertEquals(s1.getStdDeviationBound(Bounds.UPPER_SAMPLING), s2.getStdDeviationBound(Bounds.UPPER_SAMPLING), 1e-10);
    }

    @Override
    public void testSingleValuedField() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").field("value").sigma(sigma))
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        checkUpperLowerBounds(stats, sigma);
    }

    public void testSingleValuedFieldDefaultSigma() throws Exception {
        // Same as previous test, but uses a default value for sigma

        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").field("value"))
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        checkUpperLowerBounds(stats, 2);
    }

    public void testSingleValuedField_WithFormatter() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").format("0000.0").field("value").sigma(sigma))
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(stats.getAvgAsString(), equalTo("0005.5"));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMinAsString(), equalTo("0001.0"));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getMaxAsString(), equalTo("0010.0"));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(stats.getSumAsString(), equalTo("0055.0"));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100));
        assertThat(stats.getSumOfSquaresAsString(), equalTo("0385.0"));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVarianceAsString(), equalTo("0008.2"));
        assertThat(stats.getVariancePopulationAsString(), equalTo("0008.2"));
        assertThat(stats.getVarianceSamplingAsString(), equalTo("0009.2"));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationAsString(), equalTo("0002.9"));
        assertThat(stats.getStdDeviationPopulationAsString(), equalTo("0002.9"));
        assertThat(stats.getStdDeviationSamplingAsString(), equalTo("0003.0"));
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(extendedStats("stats").field("value")))
            .get();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        ExtendedStats stats = global.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        ExtendedStats statsFromProperty = (ExtendedStats) ((InternalAggregation) global).getProperty("stats");
        assertThat(statsFromProperty, notNullValue());
        assertThat(statsFromProperty, sameInstance(stats));
        double expectedAvgValue = (double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10;
        assertThat(stats.getAvg(), equalTo(expectedAvgValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.avg"), equalTo(expectedAvgValue));
        double expectedMinValue = 1.0;
        assertThat(stats.getMin(), equalTo(expectedMinValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.min"), equalTo(expectedMinValue));
        double expectedMaxValue = 10.0;
        assertThat(stats.getMax(), equalTo(expectedMaxValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.max"), equalTo(expectedMaxValue));
        double expectedSumValue = 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10;
        assertThat(stats.getSum(), equalTo(expectedSumValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.sum"), equalTo(expectedSumValue));
        long expectedCountValue = 10;
        assertThat(stats.getCount(), equalTo(expectedCountValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.count"), equalTo((double) expectedCountValue));
        double expectedSumOfSquaresValue = (double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100;
        assertThat(stats.getSumOfSquares(), equalTo(expectedSumOfSquaresValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.sum_of_squares"), equalTo(expectedSumOfSquaresValue));
        double expectedVarianceValue = variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(stats.getVariance(), equalTo(expectedVarianceValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.variance"), equalTo(expectedVarianceValue));
        double expectedVariancePopulationValue = variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(stats.getVariancePopulation(), equalTo(expectedVariancePopulationValue));
        assertThat(
            (double) ((InternalAggregation) global).getProperty("stats.variance_population"),
            equalTo(expectedVariancePopulationValue)
        );
        double expectedVarianceSamplingValue = varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(stats.getVarianceSampling(), equalTo(expectedVarianceSamplingValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.variance_sampling"), equalTo(expectedVarianceSamplingValue));
        double expectedStdDevValue = stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(stats.getStdDeviation(), equalTo(expectedStdDevValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.std_deviation"), equalTo(expectedStdDevValue));
        double expectedStdDevPopulationValue = stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(stats.getStdDeviationPopulation(), equalTo(expectedStdDevValue));
        assertThat(
            (double) ((InternalAggregation) global).getProperty("stats.std_deviation_population"),
            equalTo(expectedStdDevPopulationValue)
        );
        double expectedStdDevSamplingValue = stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(stats.getStdDeviationSampling(), equalTo(expectedStdDevSamplingValue));
        assertThat(
            (double) ((InternalAggregation) global).getProperty("stats.std_deviation_sampling"),
            equalTo(expectedStdDevSamplingValue)
        );
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").field("value").sigma(sigma))
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                extendedStats("stats").field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + 1", Collections.emptyMap()))
                    .sigma(sigma)
            )
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                extendedStats("stats").field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + inc", params))
                    .sigma(sigma)
            )
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testMultiValuedField() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").field("values").sigma(sigma))
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(
            stats.getAvg(),
            equalTo((double) (2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12) / 20)
        );
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12));
        assertThat(stats.getCount(), equalTo(20L));
        assertThat(
            stats.getSumOfSquares(),
            equalTo((double) 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121 + 144)
        );
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)));
        assertThat(
            stats.getVariancePopulation(),
            equalTo(variancePopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        );
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)));
        assertThat(
            stats.getStdDeviationPopulation(),
            equalTo(stdDevPopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        );
        assertThat(
            stats.getStdDeviationSampling(),
            equalTo(stdDevSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        );
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                extendedStats("stats").field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", Collections.emptyMap()))
                    .sigma(sigma)
            )
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(
            stats.getAvg(),
            equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11) / 20)
        );
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11));
        assertThat(stats.getCount(), equalTo(20L));
        assertThat(
            stats.getSumOfSquares(),
            equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121)
        );
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(
            stats.getVariancePopulation(),
            equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
        );
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(
            stats.getStdDeviationPopulation(),
            equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
        );
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));

        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                extendedStats("stats").field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - dec", params))
                    .sigma(sigma)
            )
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(
            stats.getAvg(),
            equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11) / 20)
        );
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11));
        assertThat(stats.getCount(), equalTo(20L));
        assertThat(
            stats.getSumOfSquares(),
            equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121)
        );
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(
            stats.getVariancePopulation(),
            equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
        );
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(
            stats.getStdDeviationPopulation(),
            equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
        );
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                extendedStats("stats").script(
                    new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value", Collections.emptyMap())
                ).sigma(sigma)
            )
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);

        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value + inc", params);

        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").script(script).sigma(sigma))
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11));
        assertThat(stats.getCount(), equalTo(10L));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviationPopulation(), equalTo(stdDevPopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)));
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                extendedStats("stats").script(
                    new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['values']", Collections.emptyMap())
                ).sigma(sigma)
            )
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(
            stats.getAvg(),
            equalTo((double) (2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12) / 20)
        );
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12));
        assertThat(stats.getCount(), equalTo(20L));
        assertThat(
            stats.getSumOfSquares(),
            equalTo((double) 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 121 + 144)
        );
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)));
        assertThat(
            stats.getVariancePopulation(),
            equalTo(variancePopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        );
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)));
        assertThat(
            stats.getStdDeviationPopulation(),
            equalTo(stdDevPopulation(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        );
        assertThat(
            stats.getStdDeviationSampling(),
            equalTo(stdDevSampling(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        );
        checkUpperLowerBounds(stats, sigma);
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);

        Script script = new Script(
            ScriptType.INLINE,
            AggregationTestScriptsPlugin.NAME,
            "[ doc['value'].value, doc['value'].value - dec ]",
            params
        );

        double sigma = randomDouble() * randomIntBetween(1, 10);
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(extendedStats("stats").script(script).sigma(sigma))
            .get();

        assertHitCount(searchResponse, 10);

        ExtendedStats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9) / 20));
        assertThat(stats.getMin(), equalTo(0.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9));
        assertThat(stats.getCount(), equalTo(20L));
        assertThat(
            stats.getSumOfSquares(),
            equalTo((double) 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 + 100 + 0 + 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81)
        );
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
        assertThat(stats.getVariancePopulation(), equalTo(variancePopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
        assertThat(stats.getVarianceSampling(), equalTo(varianceSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
        assertThat(
            stats.getStdDeviationPopulation(),
            equalTo(stdDevPopulation(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        );
        assertThat(stats.getStdDeviationSampling(), equalTo(stdDevSampling(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
        checkUpperLowerBounds(stats, sigma);
    }

    public void testEmptySubAggregation() {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("value").field("value")
                    .subAggregation(missing("values").field("values").subAggregation(extendedStats("stats").field("value")))
            )
            .get();

        assertHitCount(searchResponse, 10);

        Terms terms = searchResponse.getAggregations().get("value");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), equalTo(10));

        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket.getDocCount(), equalTo(1L));

            Missing missing = bucket.getAggregations().get("values");
            assertThat(missing, notNullValue());
            assertThat(missing.getDocCount(), equalTo(0L));

            ExtendedStats stats = missing.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getName(), equalTo("stats"));
            assertThat(stats.getSumOfSquares(), equalTo(0.0));
            assertThat(stats.getCount(), equalTo(0L));
            assertThat(stats.getSum(), equalTo(0.0));
            assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
            assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
            assertThat(Double.isNaN(stats.getStdDeviation()), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationPopulation()), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationSampling()), is(true));
            assertThat(Double.isNaN(stats.getAvg()), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER)), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER)), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER_POPULATION)), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER_POPULATION)), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.UPPER_SAMPLING)), is(true));
            assertThat(Double.isNaN(stats.getStdDeviationBound(Bounds.LOWER_SAMPLING)), is(true));
        }
    }

    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("terms").field("value")
                    .order(BucketOrder.compound(BucketOrder.aggregation("filter>extendedStats.avg", true)))
                    .subAggregation(filter("filter", termQuery("value", 100)).subAggregation(extendedStats("extendedStats").field("value")))
            )
            .get();

        assertHitCount(searchResponse, 10);

        Terms terms = searchResponse.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets.size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber(), equalTo((long) i + 1));
            assertThat(bucket.getDocCount(), equalTo(1L));
            Filter filter = bucket.getAggregations().get("filter");
            assertThat(filter, notNullValue());
            assertThat(filter.getDocCount(), equalTo(0L));
            ExtendedStats extendedStats = filter.getAggregations().get("extendedStats");
            assertThat(extendedStats, notNullValue());
            assertThat(extendedStats.getMin(), equalTo(Double.POSITIVE_INFINITY));
            assertThat(extendedStats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
            assertThat(extendedStats.getAvg(), equalTo(Double.NaN));
            assertThat(extendedStats.getSum(), equalTo(0.0));
            assertThat(extendedStats.getCount(), equalTo(0L));
            assertThat(extendedStats.getStdDeviation(), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationPopulation(), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationSampling(), equalTo(Double.NaN));
            assertThat(extendedStats.getSumOfSquares(), equalTo(0.0));
            assertThat(extendedStats.getVariance(), equalTo(Double.NaN));
            assertThat(extendedStats.getVariancePopulation(), equalTo(Double.NaN));
            assertThat(extendedStats.getVarianceSampling(), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationBound(Bounds.LOWER), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationBound(Bounds.UPPER), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationBound(Bounds.LOWER_POPULATION), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationBound(Bounds.UPPER_POPULATION), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationBound(Bounds.LOWER_SAMPLING), equalTo(Double.NaN));
            assertThat(extendedStats.getStdDeviationBound(Bounds.UPPER_SAMPLING), equalTo(Double.NaN));
        }
    }

    private void checkUpperLowerBounds(ExtendedStats stats, double sigma) {
        assertThat(stats.getStdDeviationBound(ExtendedStats.Bounds.UPPER), equalTo(stats.getAvg() + (stats.getStdDeviation() * sigma)));
        assertThat(stats.getStdDeviationBound(ExtendedStats.Bounds.LOWER), equalTo(stats.getAvg() - (stats.getStdDeviation() * sigma)));
        assertThat(
            stats.getStdDeviationBound(Bounds.UPPER_POPULATION),
            equalTo(stats.getAvg() + (stats.getStdDeviationPopulation() * sigma))
        );
        assertThat(
            stats.getStdDeviationBound(Bounds.LOWER_POPULATION),
            equalTo(stats.getAvg() - (stats.getStdDeviationPopulation() * sigma))
        );
        assertThat(stats.getStdDeviationBound(Bounds.UPPER_SAMPLING), equalTo(stats.getAvg() + (stats.getStdDeviationSampling() * sigma)));
        assertThat(stats.getStdDeviationBound(Bounds.LOWER_SAMPLING), equalTo(stats.getAvg() - (stats.getStdDeviationSampling() * sigma)));
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("cache_test_idx").setId("1").setSource("s", 1),
            client().prepareIndex("cache_test_idx").setId("2").setSource("s", 2)
        );

        // Make sure we are starting with a clear cache
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        SearchResponse r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                extendedStats("foo").field("d")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "Math.random()", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                extendedStats("foo").field("d")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + 1", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(extendedStats("foo").field("d")).get();
        assertSearchResponse(r);

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(2L)
        );
    }

}
