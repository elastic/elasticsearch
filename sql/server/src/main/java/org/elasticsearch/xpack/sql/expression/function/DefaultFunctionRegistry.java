/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.function.Score;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Correlation;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Covariance;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Kurtosis;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixCount;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixMean;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixVariance;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Mean;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRank;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Skewness;
import org.elasticsearch.xpack.sql.expression.function.aggregate.StddevPop;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.aggregate.SumOfSquares;
import org.elasticsearch.xpack.sql.expression.function.aggregate.VarPop;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfMonth;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfWeek;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.HourOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfHour;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.SecondOfMinute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Year;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ACos;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ASin;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ATan;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cbrt;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Degrees;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Expm1;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Pi;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Radians;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Tan;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class DefaultFunctionRegistry extends AbstractFunctionRegistry {
    private static final List<FunctionDefinition> FUNCTIONS = unmodifiableList(Arrays.asList(
        // Aggregate functions
            def(Avg.class, Avg::new),
            def(Count.class, Count::new),
            def(Max.class, Max::new),
            def(Min.class, Min::new),
            def(Sum.class, Sum::new),
            // Statistics
            def(Mean.class, Mean::new),
            def(StddevPop.class, StddevPop::new),
            def(VarPop.class, VarPop::new),
            def(Percentile.class, Percentile::new),
            def(PercentileRank.class, PercentileRank::new),
            def(SumOfSquares.class, SumOfSquares::new),
            // Matrix aggs
            def(MatrixCount.class, MatrixCount::new),
            def(MatrixMean.class, MatrixMean::new),
            def(MatrixVariance.class, MatrixVariance::new),
            def(Skewness.class, Skewness::new),
            def(Kurtosis.class, Kurtosis::new),
            def(Covariance.class, Covariance::new),
            def(Correlation.class, Correlation::new),
        // Scalar functions
            // Date
            def(DayOfMonth.class, DayOfMonth::new, "DAY", "DOM"),
            def(DayOfWeek.class, DayOfWeek::new, "DOW"),
            def(DayOfYear.class, DayOfYear::new, "DOY"),
            def(HourOfDay.class, HourOfDay::new, "HOUR"),
            def(MinuteOfDay.class, MinuteOfDay::new),
            def(MinuteOfHour.class, MinuteOfHour::new, "MINUTE"),
            def(SecondOfMinute.class, SecondOfMinute::new, "SECOND"),
            def(MonthOfYear.class, MonthOfYear::new, "MONTH"),
            def(Year.class, Year::new),
            // Math
            def(Abs.class, Abs::new),
            def(ACos.class, ACos::new),
            def(ASin.class, ASin::new),
            def(ATan.class, ATan::new),
            def(Cbrt.class, Cbrt::new),
            def(Ceil.class, Ceil::new),
            def(Cos.class, Cos::new),
            def(Cosh.class, Cosh::new),
            def(Degrees.class, Degrees::new),
            def(E.class, E::new),
            def(Exp.class, Exp::new),
            def(Expm1.class, Expm1::new),
            def(Floor.class, Floor::new),
            def(Log.class, Log::new),
            def(Log10.class, Log10::new),
            def(Pi.class, Pi::new),
            def(Radians.class, Radians::new),
            def(Round.class, Round::new),
            def(Sin.class, Sin::new),
            def(Sinh.class, Sinh::new),
            def(Sqrt.class, Sqrt::new),
            def(Tan.class, Tan::new),
        // Special
            def(Score.class, Score::new)));

    public DefaultFunctionRegistry() {
        super(FUNCTIONS);
    }
}
