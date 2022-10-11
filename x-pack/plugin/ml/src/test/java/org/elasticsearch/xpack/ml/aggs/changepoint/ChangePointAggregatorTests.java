/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ChangePointAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    private static final DateHistogramInterval INTERVAL = DateHistogramInterval.minutes(1);
    private static final String NUMERIC_FIELD_NAME = "value";
    private static final String TIME_FIELD_NAME = "timestamp";

    public void testNoChange() throws IOException {
        double[] bucketValues = DoubleStream.generate(() -> 10).limit(100).toArray();
        testChangeType(
            bucketValues,
            changeType -> assertThat(Arrays.toString(bucketValues), changeType, instanceOf(ChangeType.Stationary.class))
        );
    }

    public void testSlopeUp() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        AtomicInteger i = new AtomicInteger();
        double[] bucketValues = DoubleStream.generate(() -> i.addAndGet(1) + normal.sample()).limit(40).toArray();
        testChangeType(bucketValues, changeType -> {
            assertThat(changeType, instanceOf(ChangeType.NonStationary.class));
            assertThat(Arrays.toString(bucketValues), ((ChangeType.NonStationary) changeType).getTrend(), equalTo("increasing"));
        });
    }

    public void testSlopeDown() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        AtomicInteger i = new AtomicInteger(40);
        double[] bucketValues = DoubleStream.generate(() -> i.decrementAndGet() + normal.sample()).limit(40).toArray();
        testChangeType(bucketValues, changeType -> {
            assertThat(changeType, instanceOf(ChangeType.NonStationary.class));
            assertThat(Arrays.toString(bucketValues), ((ChangeType.NonStationary) changeType).getTrend(), equalTo("decreasing"));
        });
    }

    public void testSlopeChange() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 1);
        AtomicInteger i = new AtomicInteger();
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 10 + normal.sample()).limit(30),
            DoubleStream.generate(() -> (11 + 2 * i.incrementAndGet()) + normal.sample()).limit(20)
        ).toArray();
        testChangeType(bucketValues, changeType -> {
            assertThat(
                Arrays.toString(bucketValues),
                changeType,
                anyOf(instanceOf(ChangeType.TrendChange.class), instanceOf(ChangeType.NonStationary.class))
            );
            if (changeType instanceof ChangeType.NonStationary nonStationary) {
                assertThat(nonStationary.getTrend(), equalTo("increasing"));
            }
        });
    }

    public void testSpike() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 10 + normal.sample()).limit(40),
            DoubleStream.concat(DoubleStream.of(30 + normal.sample()), DoubleStream.generate(() -> 10 + normal.sample()).limit(40))
        ).toArray();
        testChangeType(bucketValues, changeType -> {
            assertThat(
                Arrays.toString(bucketValues),
                changeType,
                anyOf(instanceOf(ChangeType.Spike.class), instanceOf(ChangeType.DistributionChange.class))
            );
            if (changeType instanceof ChangeType.Spike) {
                assertThat(changeType.changePoint(), equalTo(40));
            }
        });
    }

    public void testDip() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 1);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 100 + normal.sample()).limit(40),
            DoubleStream.concat(DoubleStream.of(30 + normal.sample()), DoubleStream.generate(() -> 100 + normal.sample()).limit(40))
        ).toArray();
        testChangeType(bucketValues, changeType -> {
            assertThat(
                Arrays.toString(bucketValues),
                changeType,
                anyOf(instanceOf(ChangeType.Dip.class), instanceOf(ChangeType.DistributionChange.class))
            );
            if (changeType instanceof ChangeType.Dip) {
                assertThat(changeType.changePoint(), equalTo(40));
            }
        });
    }

    public void testStepChange() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 1);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 10 + normal.sample()).limit(20),
            DoubleStream.generate(() -> 30 + normal.sample()).limit(20)
        ).toArray();
        testChangeType(bucketValues, changeType -> {
            assertThat(Arrays.toString(bucketValues), changeType, instanceOf(ChangeType.StepChange.class));
            assertThat(changeType.changePoint(), equalTo(20));
        });
    }

    public void testDistributionChange() throws IOException {
        NormalDistribution first = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 50, 1);
        NormalDistribution second = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 50, 5);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(first::sample).limit(50),
            DoubleStream.generate(second::sample).limit(50)
        ).toArray();
        testChangeType(
            bucketValues,
            changeType -> assertThat(
                Arrays.toString(bucketValues),
                changeType,
                anyOf(
                    // Due to the random nature of the values generated, any of these could be detected
                    // Distribution change is a "catch anything weird" if previous checks didn't find anything
                    instanceOf(ChangeType.DistributionChange.class),
                    instanceOf(ChangeType.Stationary.class),
                    instanceOf(ChangeType.Spike.class),
                    instanceOf(ChangeType.Dip.class),
                    instanceOf(ChangeType.TrendChange.class)
                )
            )
        );
    }

    public void testStepChangeEdgeCaseScenarios() throws IOException {
        double[] bucketValues = new double[] {
            214505.0,
            193747.0,
            204368.0,
            193905.0,
            152777.0,
            203945.0,
            163390.0,
            163597.0,
            214807.0,
            224819.0,
            214245.0,
            21482.0,
            22264.0,
            21972.0,
            22309.0,
            21506.0,
            21365.0,
            21928.0,
            21973.0,
            23105.0,
            22118.0,
            22165.0,
            21388.0 };
        testChangeType(bucketValues, changeType -> {
            assertThat(changeType, instanceOf(ChangeType.StepChange.class));
            assertThat(Arrays.toString(bucketValues), changeType.changePoint(), equalTo(11));
        });
    }

    void testChangeType(double[] bucketValues, Consumer<ChangeType> changeTypeAssertions) throws IOException {
        FilterAggregationBuilder dummy = AggregationBuilders.filter("dummy", new MatchAllQueryBuilder())
            .subAggregation(
                new DateHistogramAggregationBuilder("time").field(TIME_FIELD_NAME)
                    .fixedInterval(INTERVAL)
                    .subAggregation(AggregationBuilders.max("max").field(NUMERIC_FIELD_NAME))
            )
            .subAggregation(new ChangePointAggregationBuilder("changes", "time>max"));
        testCase(w -> writeTestDocs(w, bucketValues), (InternalFilter result) -> {
            InternalChangePointAggregation agg = result.getAggregations().get("changes");
            changeTypeAssertions.accept(agg.getChangeType());
        }, new AggTestConfig(dummy, longField(TIME_FIELD_NAME), doubleField(NUMERIC_FIELD_NAME)));
    }

    private static void writeTestDocs(RandomIndexWriter w, double[] bucketValues) throws IOException {
        long epoch_timestamp = 0;
        for (double bucketValue : bucketValues) {
            w.addDocument(
                Arrays.asList(
                    new NumericDocValuesField(NUMERIC_FIELD_NAME, NumericUtils.doubleToSortableLong(bucketValue)),
                    new SortedNumericDocValuesField(TIME_FIELD_NAME, epoch_timestamp)
                )
            );
            epoch_timestamp += INTERVAL.estimateMillis();
        }
    }

    private static MlAggsHelper.DoubleBucketValues values(double[] values) {
        return new MlAggsHelper.DoubleBucketValues(new long[0], values, new int[0]);
    }

}
