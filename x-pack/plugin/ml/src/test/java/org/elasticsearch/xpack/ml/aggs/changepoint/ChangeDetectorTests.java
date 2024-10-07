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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ChangeDetectorTests extends AggregatorTestCase {

    public void testStationaryFalsePositiveRate() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int fp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.generate(() -> 10 + normal.sample()).limit(40).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(1e-4);
            fp += test.type() == ChangeDetector.Type.STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(10));

        fp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.generate(() -> gamma.sample()).limit(40).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(1e-4);
            fp += test.type() == ChangeDetector.Type.STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(10));
    }

    public void testSampledDistributionTestFalsePositiveRate() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0.0, 1.0);
        int fp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.generate(() -> 10 + normal.sample()).limit(5000).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(1e-4);
            fp += test.type() == ChangeDetector.Type.STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(10));
    }

    public void testNonStationaryFalsePositiveRate() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int fp = 0;
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.generate(() -> j.incrementAndGet() + normal.sample()).limit(40).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(1e-4);
            fp += test.type() == ChangeDetector.Type.NON_STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(10));

        fp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.generate(() -> j.incrementAndGet() + gamma.sample()).limit(40).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(1e-4);
            fp += test.type() == ChangeDetector.Type.NON_STATIONARY ? 0 : 1;
        }
        assertThat(fp, lessThan(10));
    }

    public void testStepChangePower() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> normal.sample()).limit(20),
                DoubleStream.generate(() -> 10 + normal.sample()).limit(20)
            ).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(0.05);
            tp += test.type() == ChangeDetector.Type.STEP_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(80));

        tp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> gamma.sample()).limit(20),
                DoubleStream.generate(() -> 10 + gamma.sample()).limit(20)
            ).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(0.05);
            tp += test.type() == ChangeDetector.Type.STEP_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(80));
    }

    public void testTrendChangePower() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> j.incrementAndGet() + normal.sample()).limit(20),
                DoubleStream.generate(() -> 2.0 * j.incrementAndGet() + normal.sample()).limit(20)
            ).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(0.05);
            tp += test.type() == ChangeDetector.Type.TREND_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(80));

        tp = 0;
        GammaDistribution gamma = new GammaDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1, 2);
        for (int i = 0; i < 100; i++) {
            AtomicInteger j = new AtomicInteger();
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> j.incrementAndGet() + gamma.sample()).limit(20),
                DoubleStream.generate(() -> 2.0 * j.incrementAndGet() + gamma.sample()).limit(20)
            ).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(0.05);
            tp += test.type() == ChangeDetector.Type.TREND_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(80));
    }

    public void testDistributionChangeTestPower() {
        NormalDistribution normal1 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0.0, 1.0);
        NormalDistribution normal2 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0.0, 10.0);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.generate(() -> 10 + normal1.sample()).limit(50),
                DoubleStream.generate(() -> 10 + normal2.sample()).limit(50)
            ).toArray();
            ChangeDetector.TestStats test = new ChangeDetector(bucketValues).testForChange(0.05);
            tp += test.type() == ChangeDetector.Type.DISTRIBUTION_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));
    }

    public void testMultipleChanges() {
        NormalDistribution normal1 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 78.0, 3.0);
        NormalDistribution normal2 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 40.0, 6.0);
        NormalDistribution normal3 = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 1.0, 0.3);
        int tp = 0;
        for (int i = 0; i < 100; i++) {
            double[] bucketValues = DoubleStream.concat(
                DoubleStream.concat(DoubleStream.generate(normal1::sample).limit(7), DoubleStream.generate(normal2::sample).limit(6)),
                DoubleStream.generate(normal3::sample).limit(23)
            ).toArray();
            ChangeDetector.TestStats result = new ChangeDetector(bucketValues).testForChange(0.05);
            tp += result.type() == ChangeDetector.Type.TREND_CHANGE ? 1 : 0;
        }
        assertThat(tp, greaterThan(90));
    }

    public void testProblemDistributionChange() {
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
        ChangeDetector.TestStats result = new ChangeDetector(bucketValues).testForChange(0.05);
        assertThat(result.type(), equalTo(ChangeDetector.Type.DISTRIBUTION_CHANGE));
    }
}
