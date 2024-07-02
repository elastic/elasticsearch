/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.Comparisons;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.ordinal;
import static org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanReader.readerFromPlanReader;
import static org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanWriter.writerFromPlanWriter;

public class In extends org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.In {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "In", In::new);

    @FunctionInfo(
        returnType = "boolean",
        description = "The `IN` operator allows testing whether a field or expression equals an element in a list of literals, "
            + "fields or expressions:",
        examples = @Example(file = "row", tag = "in-with-expressions")
    )
    public In(Source source, Expression value, List<Expression> list) {
        super(source, value, list);
    }

    private In(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            ((PlanStreamInput) in).readExpression(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression))
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        ((PlanStreamOutput) out).writeExpression(value());
        out.writeCollection(list(), writerFromPlanWriter(PlanStreamOutput::writeExpression));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.In> info() {
        return NodeInfo.create(this, In::new, value(), list());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new In(source(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    @Override
    public boolean foldable() {
        // QL's In fold()s to null, if value() is null, but isn't foldable() unless all children are
        // TODO: update this null check in QL too?
        return Expressions.isNull(value()) || super.foldable();
    }

    @Override
    public Boolean fold() {
        if (Expressions.isNull(value()) || list().stream().allMatch(Expressions::isNull)) {
            return null;
        }
        // QL's `In` fold() doesn't handle BytesRef and can't know if this is Keyword/Text, Version or IP anyway.
        // `In` allows comparisons of same type only (safe for numerics), so it's safe to apply InProcessor directly with no implicit
        // (non-numerical) conversions.
        return apply(value().fold(), list().stream().map(Expression::fold).toList());
    }

    private static Boolean apply(Object input, List<Object> values) {
        Boolean result = Boolean.FALSE;
        for (Object v : values) {
            Boolean compResult = Comparisons.eq(input, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    @Override
    protected boolean areCompatible(DataType left, DataType right) {
        if (left == DataType.UNSIGNED_LONG || right == DataType.UNSIGNED_LONG) {
            // automatic numerical conversions not applicable for UNSIGNED_LONG, see Verifier#validateUnsignedLongOperator().
            return left == right;
        }
        return EsqlDataTypes.areCompatible(left, right);
    }

    @Override
    protected TypeResolution resolveType() { // TODO: move the foldability check from QL's In to SQL's and remove this method
        TypeResolution resolution = EsqlTypeResolutions.isExact(value(), functionName(), DEFAULT);
        if (resolution.unresolved()) {
            return resolution;
        }

        DataType dt = value().dataType();
        for (int i = 0; i < list().size(); i++) {
            Expression listValue = list().get(i);
            if (areCompatible(dt, listValue.dataType()) == false) {
                return new TypeResolution(
                    format(
                        null,
                        "{} argument of [{}] must be [{}], found value [{}] type [{}]",
                        ordinal(i + 1),
                        sourceText(),
                        dt.typeName(),
                        Expressions.name(listValue),
                        listValue.dataType().typeName()
                    )
                );
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }
}
