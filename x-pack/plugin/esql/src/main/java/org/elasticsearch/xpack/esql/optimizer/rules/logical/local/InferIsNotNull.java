/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatePeriod;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVector;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToExponentialHistogram;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeohash;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeohex;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeotile;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIp;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosDecimal;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosOctal;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosRejected;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTDigest;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTimeDuration;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.UrlDecode;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.UrlEncode;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.UrlEncodeComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateDiff;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateUnitCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DayName;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.MonthName;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.ToRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.IpPrefix;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.NetworkDirection;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan2;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cbrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.CopySign;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Hypot;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Scalb;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Signum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFirst;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLast;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPercentile;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSort;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StBuffer;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StEnvelope;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohash;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohex;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeometryType;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeotile;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StIntersection;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StIsEmpty;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StNPoints;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSimplify;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSimplifyPreserveTopology;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSymDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StUnion;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StX;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StY;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.BitLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ByteLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Contains;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Hash;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Left;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Locate;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Md5;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Repeat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Reverse;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Right;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Sha1;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Sha256;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Space;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Split;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Simplify IsNotNull targets by resolving the underlying expression to its root fields.
 * e.g.
 * (x + 1) / 2 IS NOT NULL --> x IS NOT NULL AND (x+1) / 2 IS NOT NULL
 * SUBSTRING(x, 3) > 4 IS NOT NULL --> x IS NOT NULL AND SUBSTRING(x, 3) > 4 IS NOT NULL
 * When dealing with multiple fields, a conjunction/disjunction based on the predicate:
 * (x + y) / 4 IS NOT NULL --> x IS NOT NULL AND y IS NOT NULL AND (x + y) / 4 IS NOT NULL
 * This handles the case of fields nested inside functions or expressions in order to avoid:
 * - having to evaluate the whole expression
 * - not pushing down the filter due to expression evaluation
 * IS NULL cannot be simplified since it leads to a disjunction which prevents the filter to be
 * pushed down:
 * (x + 1) IS NULL --> x IS NULL OR x + 1 IS NULL
 * and x IS NULL cannot be pushed down
 * <br/>
 * Implementation-wise this rule goes bottom-up, keeping an alias up to date to the current plan
 * and then looks for replacing the target.
 */
public class InferIsNotNull extends Rule<LogicalPlan, LogicalPlan> {

    /**
     * Allowlist of functions that propagate null through all of their arguments
     * (i.e. a null in any argument forces a null result).
     * <p>
     * Does not contain (because they don't have this property):
     * <ul>
     *     <li>Case, Coalesce, In, And, Or, IsNull, IsNotNull
     *     <li>MvContains, MvInRange, MvIntersects, MvZip, MvConcat, MvAppend, MvSingleValueOrNull
     *     <li>MvPSeriesWeightedSum (a null/empty field yields 0.0, so it does not propagate null)
     * </ul>
     * <p>
     * TODO: probably add a marker interface `AnyNullIsNull` or so, and use that instead of this
     * allowlist. Verify the interface automatically in the expression/function tests.
     */
    private static final Set<Class<? extends Expression>> NULL_PROPAGATING_FUNCTIONS = Set.of(
        // arithmetic operators
        Add.class,
        Sub.class,
        Mul.class,
        Div.class,
        Mod.class,
        Neg.class,
        // comparison operators
        Equals.class,
        NotEquals.class,
        GreaterThan.class,
        GreaterThanOrEqual.class,
        LessThan.class,
        LessThanOrEqual.class,
        InsensitiveEquals.class,
        // logical operators (two-valued only; AND/OR/IN are three-valued and excluded)
        Not.class,
        // type conversion
        ToAggregateMetricDouble.class,
        ToBoolean.class,
        ToCartesianPoint.class,
        ToCartesianShape.class,
        ToCounter.class,
        ToDateNanos.class,
        ToDatePeriod.class,
        ToDateRange.class,
        ToDatetime.class,
        ToDegrees.class,
        ToDenseVector.class,
        ToDouble.class,
        ToExponentialHistogram.class,
        ToGauge.class,
        ToGeoPoint.class,
        ToGeoShape.class,
        ToGeohash.class,
        ToGeohex.class,
        ToGeotile.class,
        ToInteger.class,
        ToIpLeadingZerosDecimal.class,
        ToIpLeadingZerosOctal.class,
        ToIpLeadingZerosRejected.class,
        ToLong.class,
        ToRadians.class,
        ToString.class,
        ToTDigest.class,
        ToText.class,
        ToTimeDuration.class,
        ToUnsignedLong.class,
        ToVersion.class,
        ToIp.class,
        ToBase64.class,
        FromBase64.class,
        UrlEncode.class,
        UrlEncodeComponent.class,
        UrlDecode.class,
        FromAggregateMetricDouble.class,
        // math
        Abs.class,
        Round.class,
        RoundTo.class,
        Ceil.class,
        Floor.class,
        Signum.class,
        Sqrt.class,
        Cbrt.class,
        Exp.class,
        Log.class,
        Log10.class,
        Pow.class,
        Hypot.class,
        CopySign.class,
        Scalb.class,
        Atan2.class,
        Sin.class,
        Cos.class,
        Tan.class,
        Asin.class,
        Acos.class,
        Atan.class,
        Sinh.class,
        Cosh.class,
        Tanh.class,
        Asinh.class,
        Acosh.class,
        Atanh.class,
        // string
        Length.class,
        BitLength.class,
        ByteLength.class,
        Trim.class,
        LTrim.class,
        RTrim.class,
        ToLower.class,
        ToUpper.class,
        Substring.class,
        Left.class,
        Right.class,
        Concat.class,
        Reverse.class,
        Repeat.class,
        Space.class,
        Replace.class,
        Locate.class,
        StartsWith.class,
        EndsWith.class,
        Contains.class,
        Split.class,
        JsonExtract.class,
        Hash.class,
        Md5.class,
        Sha1.class,
        Sha256.class,
        RLike.class,
        RLikeList.class,
        WildcardLike.class,
        WildcardLikeList.class,
        // ip
        CIDRMatch.class,
        IpPrefix.class,
        NetworkDirection.class,
        // date
        DateTrunc.class,
        DateExtract.class,
        DateFormat.class,
        DateParse.class,
        DateDiff.class,
        DateUnitCount.class,
        DayName.class,
        MonthName.class,
        // spatial
        StX.class,
        StY.class,
        StXMin.class,
        StXMax.class,
        StYMin.class,
        StYMax.class,
        StEnvelope.class,
        StNPoints.class,
        StDimension.class,
        StIsEmpty.class,
        StGeometryType.class,
        StDistance.class,
        SpatialContains.class,
        SpatialDisjoint.class,
        SpatialIntersects.class,
        SpatialWithin.class,
        StDifference.class,
        StIntersection.class,
        StSymDifference.class,
        StUnion.class,
        StBuffer.class,
        StSimplify.class,
        StSimplifyPreserveTopology.class,
        StGeohash.class,
        StGeohex.class,
        StGeotile.class,
        // range (date/numeric range types)
        RangeMin.class,
        RangeMax.class,
        RangeIntersects.class,
        RangeWithin.class,
        ToRange.class,
        // conditional min/max
        Greatest.class,
        Least.class,
        ClampMax.class,
        ClampMin.class,
        // multivalue reducers
        MvMin.class,
        MvMax.class,
        MvSum.class,
        MvAvg.class,
        MvMedian.class,
        MvMedianAbsoluteDeviation.class,
        MvFirst.class,
        MvLast.class,
        MvCount.class,
        MvDedupe.class,
        MvSort.class,
        MvSlice.class,
        MvPercentile.class
    );

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        // the alias map is shared across the whole plan
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        // traverse bottom-up to pick up the aliases as we go
        plan = plan.transformUp(p -> inspectPlan(p, aliasesBuilder));
        return plan;
    }

    private LogicalPlan inspectPlan(LogicalPlan plan, AttributeMap.Builder<Expression> aliasesBuilder) {
        // inspect just this plan properties
        plan.forEachExpression(Alias.class, a -> aliasesBuilder.put(a.toAttribute(), a.child()));
        // now go about finding isNull/isNotNull
        LogicalPlan newPlan = plan.transformExpressionsOnlyUp(IsNotNull.class, inn -> inferNotNullable(inn, aliasesBuilder.build()));
        return newPlan;
    }

    private Expression inferNotNullable(IsNotNull inn, AttributeMap<Expression> aliases) {
        Expression result = inn;
        Set<Expression> refs = resolveExpressionAsRootAttributes(inn.field(), aliases);
        // no refs found or could not detect - return the original function
        if (refs.size() > 0) {
            // add IsNull for the filters along with the initial inn
            var innList = CollectionUtils.combine(refs.stream().map(r -> (Expression) new IsNotNull(inn.source(), r)).toList(), inn);
            result = Predicates.combineAnd(innList);
        }
        return result;
    }

    private Set<Expression> resolveExpressionAsRootAttributes(Expression exp, AttributeMap<Expression> aliases) {
        Expression resolved = aliases.resolve(exp, exp);
        if (exp instanceof Attribute && resolved == exp) {
            return emptySet();
        }
        Set<Expression> resolvedExpressions = new LinkedHashSet<>();
        resolve(resolved, aliases, resolvedExpressions);
        return resolvedExpressions;
    }

    private void resolve(Expression exp, AttributeMap<Expression> aliases, Set<Expression> resolvedExpressions) {
        Expression resolved = aliases.resolve(exp, exp);
        if (resolved instanceof Attribute a) {
            resolvedExpressions.add(a);
            return;
        }
        if (NULL_PROPAGATING_FUNCTIONS.contains(resolved.getClass()) == false) {
            return;
        }
        for (Expression child : resolved.children()) {
            resolve(child, aliases, resolvedExpressions);
        }
    }
}
