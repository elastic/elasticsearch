/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.Build;
import org.elasticsearch.test.ListMatcher;

import java.util.Collection;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;

/**
 * Makes sure all functions inside the main ESQL module have all required tests.
 */
public class EsqlFunctionsHaveTests extends AbstractFunctionsHaveTestsCase {
    @Override
    public void testRegisteredFunctionHaveTests() {
        assumeTrue("""
            Some functions are missing when outside of snapshot and we really
            only care about the superset of all functions. Just skip this test
            on release builds.""", Build.current().isSnapshot());
        super.testRegisteredFunctionHaveTests();
    }

    @Override
    protected Collection<FunctionDefinition> functionDefinitions() {
        return TEST_FUNCTION_REGISTRY.snapshotRegistry().listFunctions();
    }

    @Override
    protected ListMatcher knownMissingTests() {
        /*
         * We should add these missing tests when we can. It's *fine* to add to this list
         * while working on something, but released stuff should not have an entry here.
         */
        return matchesList().item("org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinctErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinctOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.DeltaErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.DerivErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.IdeltaErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.IncreaseErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.IrateErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviationErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.MedianErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTimeSerializationTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.RateErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SparklineErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroidErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtentErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.StdDevErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTimeSerializationTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SumErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTimeErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTimeSerializationTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvgErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.fulltext.ScoreErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.grouping.BucketErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.grouping.TBucketErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithoutErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithoutTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.ClampErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.ClampTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMaxErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMinErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.GreatestErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.conditional.LeastErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDoubleErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVectorErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersionErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersectionErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSortErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.nulls.CoalesceErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.string.ContainsErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.util.DelayErrorTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.scalar.util.DelayTests is missing")
            .item("org.elasticsearch.xpack.esql.expression.function.vector.MagnitudeErrorTests is missing");
    }
}
