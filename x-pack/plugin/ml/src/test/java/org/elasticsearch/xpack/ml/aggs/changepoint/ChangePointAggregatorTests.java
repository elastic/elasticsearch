/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;

import static org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointAggregator.P_VALUE_THRESHOLD;
import static org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointAggregator.candidateChangePoints;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ChangePointAggregatorTests extends ESTestCase {

    public void testNoChange() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 0.5);
        double[] bucketValues = DoubleStream.generate(() -> 10 + normal.sample()).limit(60).toArray();
        assertThat(
            Arrays.toString(bucketValues),
            ChangePointAggregator.changePValue(values(bucketValues), candidateChangePoints(bucketValues), 1e-5),
            instanceOf(ChangeType.Stationary.class)
        );
    }

    public void testSlopeUp() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        AtomicInteger i = new AtomicInteger();
        double[] bucketValues = DoubleStream.generate(() -> i.addAndGet(1) + normal.sample()).limit(40).toArray();
        ChangeType changeType = ChangePointAggregator.changePValue(
            values(bucketValues),
            candidateChangePoints(bucketValues),
            P_VALUE_THRESHOLD
        );
        assertThat(changeType, instanceOf(ChangeType.NonStationary.class));
        assertThat(Arrays.toString(bucketValues), ((ChangeType.NonStationary) changeType).getTrend(), equalTo("increasing"));
    }

    public void testSlopeDown() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        AtomicInteger i = new AtomicInteger(40);
        double[] bucketValues = DoubleStream.generate(() -> i.decrementAndGet() + normal.sample()).limit(40).toArray();
        ChangeType changeType = ChangePointAggregator.changePValue(
            values(bucketValues),
            candidateChangePoints(bucketValues),
            P_VALUE_THRESHOLD
        );
        assertThat(changeType, instanceOf(ChangeType.NonStationary.class));
        assertThat(Arrays.toString(bucketValues), ((ChangeType.NonStationary) changeType).getTrend(), equalTo("decreasing"));
    }

    public void testSlopeChange() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 1);
        AtomicInteger i = new AtomicInteger();
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 10 + normal.sample()).limit(30),
            DoubleStream.generate(() -> (11 + 2 * i.incrementAndGet()) + normal.sample()).limit(20)
        ).toArray();
        ChangeType changeType = ChangePointAggregator.changePValue(
            values(bucketValues),
            candidateChangePoints(bucketValues),
            P_VALUE_THRESHOLD
        );
        assertThat(
            Arrays.toString(bucketValues),
            changeType,
            anyOf(instanceOf(ChangeType.TrendChange.class), instanceOf(ChangeType.NonStationary.class))
        );
        if (changeType instanceof ChangeType.NonStationary nonStationary) {
            assertThat(nonStationary.getTrend(), equalTo("increasing"));
        }
    }

    public void testSpike() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 2);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 10 + normal.sample()).limit(25),
            DoubleStream.concat(DoubleStream.of(30 + normal.sample()), DoubleStream.generate(() -> 10 + normal.sample()).limit(14))
        ).toArray();
        ChangeType type = ChangePointAggregator.maxDeviationNormalModelPValue(values(bucketValues), P_VALUE_THRESHOLD);
        assertThat(Arrays.toString(bucketValues), type, instanceOf(ChangeType.Spike.class));
        assertThat(type.changePoint(), equalTo(25));
    }

    public void testDip() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 1);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 100 + normal.sample()).limit(25),
            DoubleStream.concat(DoubleStream.of(30 + normal.sample()), DoubleStream.generate(() -> 100 + normal.sample()).limit(14))
        ).toArray();
        ChangeType type = ChangePointAggregator.maxDeviationNormalModelPValue(values(bucketValues), P_VALUE_THRESHOLD);
        assertThat(Arrays.toString(bucketValues), type, instanceOf(ChangeType.Dip.class));
        assertThat(type.changePoint(), equalTo(25));
    }

    public void testStepChange() {
        NormalDistribution normal = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 0, 1);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(() -> 10 + normal.sample()).limit(20),
            DoubleStream.generate(() -> 30 + normal.sample()).limit(20)
        ).toArray();
        ChangeType type = ChangePointAggregator.changePValue(values(bucketValues), candidateChangePoints(bucketValues), P_VALUE_THRESHOLD);
        assertThat(type, instanceOf(ChangeType.StepChange.class));
    }

    public void testDistributionChange() {
        NormalDistribution first = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 50, 1);
        NormalDistribution second = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(Randomness.get()), 50, 10);
        double[] bucketValues = DoubleStream.concat(
            DoubleStream.generate(first::sample).limit(30),
            DoubleStream.generate(second::sample).limit(30)
        ).toArray();
        ChangeType type = ChangePointAggregator.changePValue(values(bucketValues), candidateChangePoints(bucketValues), P_VALUE_THRESHOLD);
        assertThat(
            Arrays.toString(bucketValues),
            type,
            anyOf(instanceOf(ChangeType.DistributionChange.class), instanceOf(ChangeType.Stationary.class))
        );
    }

    private static MlAggsHelper.DoubleBucketValues values(double[] values) {
        return new MlAggsHelper.DoubleBucketValues(new long[0], values, new int[0]);
    }
}
