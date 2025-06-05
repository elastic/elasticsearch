/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.ExpressionCoreWritables;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateWritables;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextWritables;
import org.elasticsearch.xpack.esql.expression.function.scalar.ScalarFunctionWritables;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIP;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cbrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Scalb;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Signum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFunctionWritables;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StEnvelope;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StX;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StY;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ByteLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Space;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.util.Delay;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;

public class ExpressionWritables {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        entries.addAll(allExpressions());
        entries.addAll(aggregates());
        entries.addAll(scalars());
        entries.addAll(spatials());
        entries.addAll(arithmetics());
        entries.addAll(binaryComparisons());
        entries.addAll(fullText());
        entries.addAll(unaryScalars());
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> attributes() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(ExpressionCoreWritables.attributes());
        entries.add(UnsupportedAttribute.ENTRY);
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> namedExpressions() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(ExpressionCoreWritables.namedExpressions());
        entries.add(UnsupportedAttribute.NAMED_EXPRESSION_ENTRY);
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> expressions() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(ExpressionCoreWritables.expressions());
        entries.add(UnsupportedAttribute.EXPRESSION_ENTRY);
        entries.add(Order.ENTRY);
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> allExpressions() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(expressions());
        entries.addAll(namedExpressions());
        entries.addAll(attributes());
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> aggregates() {
        return AggregateWritables.getNamedWriteables();
    }

    public static List<NamedWriteableRegistry.Entry> scalars() {
        return ScalarFunctionWritables.getNamedWriteables();
    }

    public static List<NamedWriteableRegistry.Entry> unaryScalars() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(Abs.ENTRY);
        entries.add(Acos.ENTRY);
        entries.add(Asin.ENTRY);
        entries.add(Atan.ENTRY);
        entries.add(ByteLength.ENTRY);
        entries.add(Cbrt.ENTRY);
        entries.add(Ceil.ENTRY);
        entries.add(Cos.ENTRY);
        entries.add(Cosh.ENTRY);
        entries.add(Exp.ENTRY);
        entries.add(Floor.ENTRY);
        entries.add(FromBase64.ENTRY);
        entries.add(IsNotNull.ENTRY);
        entries.add(IsNull.ENTRY);
        entries.add(Length.ENTRY);
        entries.add(Log10.ENTRY);
        entries.add(LTrim.ENTRY);
        entries.add(Neg.ENTRY);
        entries.add(Not.ENTRY);
        entries.add(RLike.ENTRY);
        entries.add(RTrim.ENTRY);
        entries.add(Scalb.ENTRY);
        entries.add(Signum.ENTRY);
        entries.add(Sin.ENTRY);
        entries.add(Sinh.ENTRY);
        entries.add(Space.ENTRY);
        entries.add(Sqrt.ENTRY);
        entries.add(StEnvelope.ENTRY);
        entries.add(StXMax.ENTRY);
        entries.add(StXMin.ENTRY);
        entries.add(StYMax.ENTRY);
        entries.add(StYMin.ENTRY);
        entries.add(StX.ENTRY);
        entries.add(StY.ENTRY);
        entries.add(Tan.ENTRY);
        entries.add(Tanh.ENTRY);
        entries.add(ToAggregateMetricDouble.ENTRY);
        entries.add(ToBase64.ENTRY);
        entries.add(ToBoolean.ENTRY);
        entries.add(ToCartesianPoint.ENTRY);
        entries.add(ToDatetime.ENTRY);
        entries.add(ToDateNanos.ENTRY);
        entries.add(ToDegrees.ENTRY);
        entries.add(ToDouble.ENTRY);
        entries.add(ToGeoShape.ENTRY);
        entries.add(ToCartesianShape.ENTRY);
        entries.add(ToGeoPoint.ENTRY);
        entries.add(ToIP.ENTRY);
        entries.add(ToInteger.ENTRY);
        entries.add(ToLong.ENTRY);
        entries.add(ToRadians.ENTRY);
        entries.add(ToString.ENTRY);
        entries.add(ToUnsignedLong.ENTRY);
        entries.add(ToVersion.ENTRY);
        entries.add(Trim.ENTRY);
        entries.add(WildcardLike.ENTRY);
        entries.add(Delay.ENTRY);
        // mv functions
        entries.addAll(MvFunctionWritables.getNamedWriteables());
        return entries;
    }

    private static List<NamedWriteableRegistry.Entry> spatials() {
        return List.of(SpatialContains.ENTRY, SpatialDisjoint.ENTRY, SpatialIntersects.ENTRY, SpatialWithin.ENTRY, StDistance.ENTRY);
    }

    private static List<NamedWriteableRegistry.Entry> arithmetics() {
        return List.of(Add.ENTRY, Div.ENTRY, Mod.ENTRY, Mul.ENTRY, Sub.ENTRY);
    }

    private static List<NamedWriteableRegistry.Entry> binaryComparisons() {
        return List.of(Equals.ENTRY, GreaterThan.ENTRY, GreaterThanOrEqual.ENTRY, LessThan.ENTRY, LessThanOrEqual.ENTRY, NotEquals.ENTRY);
    }

    private static List<NamedWriteableRegistry.Entry> fullText() {
        return FullTextWritables.getNamedWriteables();
    }
}
