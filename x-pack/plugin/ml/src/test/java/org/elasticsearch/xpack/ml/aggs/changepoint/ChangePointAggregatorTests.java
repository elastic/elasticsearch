/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.distribution.GammaDistribution;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class ChangePointAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    private static final DateHistogramInterval INTERVAL = DateHistogramInterval.minutes(1);
    private static final String NUMERIC_FIELD_NAME = "value";
    private static final String TIME_FIELD_NAME = "timestamp";

    public void testStationaryFalsePositives() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int fp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.generate(() -> 10 + normal.sample()).limit(40).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 1e-3);
            fp += test.type() == ChangePointAggregator.Type.STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(5));

        fp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.generate(() -> gamma.sample()).limit(40).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 1e-3);
            fp += test.type() == ChangePointAggregator.Type.STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(5));
    }

    public void testNonStationaryFalsePositives() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int fp = 0;
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.generate(() -> j.incrementAndGet() + normal.sample()).limit(40).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 1e-3);
            fp += test.type() == ChangePointAggregator.Type.NON_STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(5));

        fp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.generate(() -> j.incrementAndGet() + gamma.sample()).limit(40).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 1e-3);
            fp += test.type() == ChangePointAggregator.Type.NON_STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(5));
    }

    public void testStepChangePower() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> normal.sample()).limit(20),
                DoubleStream.generate(() -> 10 + normal.sample()).limit(20)
            ).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 0.05);
            tp += test.type() == ChangePointAggregator.Type.STEP_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));

        tp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> gamma.sample()).limit(20),
                DoubleStream.generate(() -> 10 + gamma.sample()).limit(20)
            ).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 0.05);
            tp += test.type() == ChangePointAggregator.Type.STEP_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));
    }

    public void testTrendChangePower() throws IOException {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> j.incrementAndGet() + normal.sample()).limit(20),
                DoubleStream.generate(() -> 2.0 * j.incrementAndGet() + normal.sample()).limit(20)
            ).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 0.05);
            tp += test.type() == ChangePointAggregator.Type.TREND_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));

        tp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> j.incrementAndGet() + gamma.sample()).limit(20),
                DoubleStream.generate(() -> 2.0 * j.incrementAndGet() + gamma.sample()).limit(20)
            ).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 0.05);
            tp += test.type() == ChangePointAggregator.Type.TREND_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));
    }

    public void testDistributionChangeTestPower() throws IOException {
        NormalDistribution normal1 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0.0, 1.0);
        NormalDistribution normal2 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0.0, 10.0);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> 10 + normal1.sample()).limit(50),
                DoubleStream.generate(() -> 10 + normal2.sample()).limit(50)
            ).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats test = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 0.05);
            tp += test.type() == ChangePointAggregator.Type.DISTRIBUTION_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));
    }

    public void testMultipleChanges() throws IOException {
        NormalDistribution normal1 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 78.0, 3.0);
        NormalDistribution normal2 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 40.0, 6.0);
        NormalDistribution normal3 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1.0, 0.3);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.concat(
                    DoubleStream.generate(() -> normal1.sample()).limit(7),
                    DoubleStream.generate(() -> normal2.sample()).limit(6)
                ),
                DoubleStream.generate(() -> normal3.sample()).limit(23)
            ).toArray();
            int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
            ChangePointAggregator.TestStats result = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 0.05);
            tp += result.type() == ChangePointAggregator.Type.TREND_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));
    }

    public void testProblemDistributionChange() throws IOException {
        double[] bucketValues = new double[] {
            546.3651753325270,
            550.872738079514,
            551.1312487618040,
            550.3323904749380,
            549.2652495378930,
            548.9761274963630,
            549.3433969743010,
            549.0935313531350,
            551.1762550747600,
            551.3772184469220,
            548.6163495094490,
            548.5866591594080,
            546.9364791288570,
            548.1167839989470,
            549.3484016149320,
            550.4242803917040,
            551.2316023050940,
            548.4713993534340,
            546.0254901960780,
            548.4376996805110,
            561.1920529801320,
            557.3930041152260,
            565.8497217068650,
            566.787072243346,
            546.6094890510950,
            530.5905797101450,
            556.7340823970040,
            557.3857677902620,
            543.0754716981130,
            574.3297101449280,
            559.2962962962960,
            549.5202952029520,
            531.7217741935480,
            551.4333333333330,
            557.637168141593,
            545.1880733944950,
            564.6893203883500,
            543.0204081632650,
            571.820809248555,
            541.2589928057550,
            520.4387755102040 };
        int[] candidatePoints = ChangePointAggregator.candidateChangePoints(bucketValues);
        ChangePointAggregator.TestStats result = ChangePointAggregator.testForChange(bucketValues, candidatePoints, 0.05);
        assertThat(result.type(), equalTo(ChangePointAggregator.Type.DISTRIBUTION_CHANGE));
    }

    public void testConstant() throws IOException {
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
        NormalDistribution first = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 1);
        NormalDistribution second = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 5);
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

    public void testZeroDeviation() throws IOException {
        double[] bucketValues = DoubleStream.generate(() -> 4243.1621621621625).limit(30).toArray();
        testChangeType(bucketValues, changeType -> { assertThat(changeType, instanceOf(ChangeType.Stationary.class)); });
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

}
