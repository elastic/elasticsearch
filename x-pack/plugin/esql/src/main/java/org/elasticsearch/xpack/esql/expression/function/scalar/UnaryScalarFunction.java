/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromBase64;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Signum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StX;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StY;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Space;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;

public abstract class UnaryScalarFunction extends EsqlScalarFunction {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(Abs.ENTRY);
        entries.add(Acos.ENTRY);
        entries.add(Asin.ENTRY);
        entries.add(Atan.ENTRY);
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
        entries.add(Signum.ENTRY);
        entries.add(Sin.ENTRY);
        entries.add(Sinh.ENTRY);
        entries.add(Space.ENTRY);
        entries.add(Sqrt.ENTRY);
        entries.add(StX.ENTRY);
        entries.add(StY.ENTRY);
        entries.add(Tan.ENTRY);
        entries.add(Tanh.ENTRY);
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
        entries.addAll(AbstractMultivalueFunction.getNamedWriteables());
        return entries;
    }

    protected final Expression field;

    public UnaryScalarFunction(Source source, Expression field) {
        super(source, Arrays.asList(field));
        this.field = field;
    }

    protected UnaryScalarFunction(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    public final Expression field() {
        return field;
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }
}
