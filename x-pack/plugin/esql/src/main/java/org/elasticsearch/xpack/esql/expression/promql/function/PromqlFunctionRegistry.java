/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Delta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Deriv;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Idelta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Increase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Irate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Variance;
import org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTime;
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pi;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Signum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A registry for PromQL functions that maps function names to their respective definitions.
 */
public class PromqlFunctionRegistry {

    private static final PromqlFunctionDefinition[] FUNCTION_DEFINITIONS = new PromqlFunctionDefinition[] {
        //
        Delta.PROMQL_DEFINITION,
        Idelta.PROMQL_DEFINITION,
        Increase.PROMQL_DEFINITION,
        Irate.PROMQL_DEFINITION,
        Rate.PROMQL_DEFINITION,
        FirstOverTime.PROMQL_DEFINITION,
        LastOverTime.PROMQL_DEFINITION,
        Deriv.PROMQL_DEFINITION,
        //
        AvgOverTime.PROMQL_DEFINITION,
        CountOverTime.PROMQL_DEFINITION,
        MaxOverTime.PROMQL_DEFINITION,
        MinOverTime.PROMQL_DEFINITION,
        SumOverTime.PROMQL_DEFINITION,
        StddevOverTime.PROMQL_DEFINITION,
        VarianceOverTime.PROMQL_DEFINITION,
        AbsentOverTime.PROMQL_DEFINITION,
        PresentOverTime.PROMQL_DEFINITION,
        //
        PercentileOverTime.PROMQL_DEFINITION,
        //
        Avg.PROMQL_DEFINITION,
        Count.PROMQL_DEFINITION,
        Max.PROMQL_DEFINITION,
        Min.PROMQL_DEFINITION,
        Sum.PROMQL_DEFINITION,
        StdDev.PROMQL_DEFINITION,
        Variance.PROMQL_DEFINITION,
        //
        Percentile.PROMQL_DEFINITION,
        //
        Ceil.PROMQL_DEFINITION,
        Abs.PROMQL_DEFINITION,
        Signum.PROMQL_DEFINITION,
        Exp.PROMQL_DEFINITION,
        Sqrt.PROMQL_DEFINITION,
        Log10.PROMQL_DEFINITION,
        Log.PROMQL_LOG2_DEFINITION,
        Log.PROMQL_LN_DEFINITION,
        Floor.PROMQL_DEFINITION,
        Round.PROMQL_DEFINITION,
        //
        Asin.PROMQL_DEFINITION,
        Acos.PROMQL_DEFINITION,
        Atan.PROMQL_DEFINITION,
        Cos.PROMQL_DEFINITION,
        Cosh.PROMQL_DEFINITION,
        Acosh.PROMQL_DEFINITION,
        Asinh.PROMQL_DEFINITION,
        Atanh.PROMQL_DEFINITION,
        Sinh.PROMQL_DEFINITION,
        Sin.PROMQL_DEFINITION,
        Tan.PROMQL_DEFINITION,
        Tanh.PROMQL_DEFINITION,
        ToDegrees.PROMQL_DEFINITION,
        ToRadians.PROMQL_DEFINITION,
        ClampMin.PROMQL_DEFINITION,
        ClampMax.PROMQL_DEFINITION,
        Clamp.PROMQL_DEFINITION,
        //
        PromqlBuiltinFunctionDefinitions.VECTOR,
        PromqlBuiltinFunctionDefinitions.SCALAR,
        Pi.PROMQL_DEFINITION,
        PromqlBuiltinFunctionDefinitions.YEAR,
        PromqlBuiltinFunctionDefinitions.MONTH,
        PromqlBuiltinFunctionDefinitions.DAY_OF_MONTH,
        PromqlBuiltinFunctionDefinitions.DAY_OF_WEEK,
        PromqlBuiltinFunctionDefinitions.DAY_OF_YEAR,
        PromqlBuiltinFunctionDefinitions.HOUR,
        PromqlBuiltinFunctionDefinitions.MINUTE,
        PromqlBuiltinFunctionDefinitions.TIME, };

    public static final PromqlFunctionRegistry INSTANCE = new PromqlFunctionRegistry();

    private final Map<String, PromqlFunctionDefinition> promqlFunctions = new HashMap<>();

    private PromqlFunctionRegistry() {
        for (PromqlFunctionDefinition def : FUNCTION_DEFINITIONS) {
            String normalized = normalize(def.name());
            promqlFunctions.put(normalized, def);
        }
    }

    /**
     * Carries the PromQL evaluation context needed by function builders to construct ES|QL expressions.
     */
    public record PromqlContext(Expression timestamp, Expression window, Expression step, Configuration configuration) {}

    // PromQL function names not yet implemented
    // https://github.com/elastic/metrics-program/issues/39
    private static final Set<String> NOT_IMPLEMENTED = Set.of(
        // Across-series aggregations (not yet available in ESQL)
        "bottomk",
        "topk",
        "group",
        "count_values",

        // Range vector functions (not yet implemented)
        "changes",
        "holt_winters",
        "mad_over_time",
        "predict_linear",
        "resets",

        // Instant vector functions
        "absent",
        "sort",
        "sort_desc",

        // Time functions
        "days_in_month",
        "timestamp",

        // Label manipulation functions
        "label_join",
        "label_replace",

        // Histogram functions
        "histogram_avg",
        "histogram_count",
        "histogram_fraction",
        "histogram_quantile",
        "histogram_stddev",
        "histogram_stdvar",
        "histogram_sum"
    );

    private String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    public Collection<PromqlFunctionDefinition> allFunctions() {
        return new ArrayList<>(promqlFunctions.values());
    }

    /**
     * Retrieves the function definition metadata for the given function name.
     */
    public PromqlFunctionDefinition functionMetadata(String name) {
        String normalized = normalize(name);
        return promqlFunctions.get(normalized);
    }

    /**
     * Returns {@code true} if the function with the given name exists in the registry but
     * has not yet been implemented.
     */
    public boolean isNotImplemented(String name) {
        return NOT_IMPLEMENTED.contains(normalize(name));
    }

    public void checkFunction(Source source, String name) {
        String normalized = normalize(name);

        if (promqlFunctions.containsKey(normalized) == false) {
            throw new ParsingException(source, "Function [{}] does not exist", name);
        }

        if (NOT_IMPLEMENTED.contains(normalized)) {
            throw new ParsingException(source, "Function [{}] is not yet implemented", name);
        }
    }

    public Expression buildEsqlFunction(String name, Source source, Expression target, PromqlContext ctx, List<Expression> extraParams) {
        checkFunction(source, name);
        PromqlFunctionDefinition metadata = functionMetadata(name);
        try {
            return metadata.esqlBuilder().build(source, target, ctx, extraParams);
        } catch (Exception e) {
            throw new ParsingException(source, "Error building ESQL function for [{}]: {}", name, e.getMessage());
        }
    }
}
