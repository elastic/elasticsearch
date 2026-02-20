/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Absent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinctOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DefaultTimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Delta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Deriv;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMerge;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMergeOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Idelta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Increase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Irate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.NumericAggregate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTime;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MMR;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.fuse.FuseScoreEval;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ValueTransformationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.VectorConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryArithmetic;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;

import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * These tests verify that each LogicalPlan and AggregateFunction is either explicitly
 * supported for approximation (by being on a whitelist in {@link Approximation}) or
 * explicitly not supported (by being on a blacklist here).
 * This forces a conscious decision about whether LogicalPlans and AggregateFunctions
 * are supported for approximation or not.
 */
public class ApproximationSupportTests extends ESTestCase {

    private static final Set<Class<? extends LogicalPlan>> UNSUPPORTED_COMMANDS = Set.of(
        // TODO: investigate whether these plans are supported or explain why not
        Fuse.class,
        FuseScoreEval.class,
        Lookup.class,
        MMR.class,
        Subquery.class,

        // Non-unary plans are not supported yet.
        // These require more complicated expression tree traversal.
        Fork.class,
        UnionAll.class,
        Join.class,
        InlineJoin.class,
        LookupJoin.class,

        // InlineStats is not supported yet.
        // Only a single Stats command is supported.
        InlineStats.class,

        // Timeseries indices are not supported yet.
        // They require chained Stats commands.
        TimeSeriesAggregate.class,

        // These source commands makes no sense for approximation.
        Explain.class,
        ShowInfo.class,
        LocalRelation.class,
        MetricsInfo.class,
        ExternalRelation.class,

        // The plans are superclasses of other plans.
        LogicalPlan.class,
        LeafPlan.class,
        UnaryPlan.class,
        BinaryPlan.class,
        InferencePlan.class,
        CompoundOutputEval.class,

        // These plans don't occur in a correct analyzed query.
        UnresolvedRelation.class,
        UnresolvedExternalRelation.class,
        StubRelation.class,
        Drop.class,
        Keep.class,
        Rename.class,
        ResolvingProject.class,

        // PromQL plans are not supported yet.
        PromqlCommand.class,
        LiteralSelector.class,
        Selector.class,
        InstantSelector.class,
        RangeSelector.class,
        PromqlFunctionCall.class,
        WithinSeriesAggregate.class,
        AcrossSeriesAggregate.class,
        PlaceholderRelation.class,
        ScalarConversionFunction.class,
        ScalarFunction.class,
        ValueTransformationFunction.class,
        VectorBinarySet.class,
        VectorBinaryArithmetic.class,
        VectorBinaryComparison.class,
        VectorBinaryOperator.class,
        VectorConversionFunction.class
    );

    private static final Set<Class<? extends AggregateFunction>> UNSUPPORTED_AGGS = Set.of(
        // TODO: investigate whether these aggs are supported or explain why not
        HistogramMerge.class,

        // Counting distinct values is hard to approximate.
        // For more details, see:
        // - https://arxiv.org/pdf/2202.02800
        CountDistinct.class,

        // Aggs that produce minimums or maximums, firsts or lasts, presence or absence
        // don't behave well under approximation (bad convergence under CLT).
        // For more details, see:
        // - https://en.wikipedia.org/wiki/Extreme_value_theory
        // - https://en.wikipedia.org/wiki/Generalized_extreme_value_distribution
        FirstDocId.class,
        First.class,
        Last.class,
        Max.class,
        Min.class,
        Absent.class,
        Present.class,
        Top.class,

        // Spatial aggs are not supported for approximation.
        SpatialExtent.class,
        SpatialCentroid.class,

        // These multi-valued aggs are not suitable for approximation.
        DimensionValues.class,
        Values.class,

        // These aggs are superclasses of other aggs.
        AggregateFunction.class,
        NumericAggregate.class,
        TimeSeriesAggregateFunction.class,
        SpatialAggregateFunction.class,

        // These aggs don't occur in a correct query.
        SumSerializationTests.OldSum.class,
        AvgSerializationTests.OldAvg.class,

        // Time series aggregates are not supported yet.
        AbsentOverTime.class,
        AvgOverTime.class,
        CountDistinctOverTime.class,
        CountOverTime.class,
        DefaultTimeSeriesAggregateFunction.class,
        Delta.class,
        Deriv.class,
        FirstOverTime.class,
        Idelta.class,
        Increase.class,
        Irate.class,
        HistogramMergeOverTime.class,
        LastOverTime.class,
        MaxOverTime.class,
        MinOverTime.class,
        PercentileOverTime.class,
        PresentOverTime.class,
        Rate.class,
        StddevOverTime.class,
        SumOverTime.class,
        VarianceOverTime.class,
        Scalar.class
    );

    private static List<Class<?>> getClassesInPackage(String packageName) throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String path = packageName.replace('.', '/');

        // Get all directory paths for this package
        Enumeration<URL> resources = loader.getResources(path);
        while (resources.hasMoreElements()) {
            Path directory = PathUtils.get(resources.nextElement().toURI());
            if (Files.exists(directory) && Files.isDirectory(directory)) {
                scan(directory, packageName, classes);
            }
        }
        return classes;
    }

    private static void scan(Path directory, String pkg, List<Class<?>> classes) throws Exception {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    scan(entry, pkg + "." + entry.getFileName().toString(), classes);
                } else if (entry.getFileName().toString().endsWith(".class")) {
                    String className = pkg + "." + entry.getFileName().toString().replace(".class", "");
                    classes.add(Class.forName(className));
                }
            }
        }
    }

    public void testAllCommandsWhitelistedOrBlacklisted() throws Exception {
        testAllClassesListed(LogicalPlan.class, List.of(Approximation.SUPPORTED_COMMANDS, UNSUPPORTED_COMMANDS));
    }

    public void testAllAggregationsWhitelistedOrBlacklisted() throws Exception {
        testAllClassesListed(
            AggregateFunction.class,
            List.of(Approximation.SUPPORTED_SINGLE_VALUED_AGGS, Approximation.SUPPORTED_MULTIVALUED_AGGS, UNSUPPORTED_AGGS)
        );
    }

    /**
     * Extracts all classes that are subclasses of the given class from the classpath, and
     * verifies that all of them are included in exactly one of the given sets of classes.
     */
    private <T> void testAllClassesListed(Class<T> clazz, List<Set<Class<? extends T>>> listedClasses) throws Exception {
        Set<Class<? extends T>> allListedClasses = new HashSet<>();
        for (Set<Class<? extends T>> list : listedClasses) {
            assertThat(Sets.intersection(allListedClasses, list), empty());
            allListedClasses.addAll(list);
        }
        Set<Class<? extends T>> classesOnClassPath = getClassesInPackage(clazz.getPackageName()).stream()
            .filter(clazz::isAssignableFrom)
            .map(c -> (Class<? extends T>) c.asSubclass(clazz))
            .collect(Collectors.toSet());
        assertThat(
            "missing classes: " + Sets.difference(classesOnClassPath, allListedClasses),
            classesOnClassPath,
            equalTo(allListedClasses)
        );
    }
}
