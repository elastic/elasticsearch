/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;

public abstract class UnaryScalarFunction extends EsqlScalarFunction {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
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
        entries.add(Delay.ENTRY);
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
        return field.dataType().noText();
    }
}
