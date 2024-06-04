/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.Comparisons;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.ordinal;

public class In extends org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.In implements EvaluatorMapper {
    @FunctionInfo(
        returnType = "boolean",
        description = "The `IN` operator allows testing whether a field or expression equals an element in a list of literals, "
            + "fields or expressions:",
        examples = @Example(file = "row", tag = "in-with-expressions")
    )
    public In(Source source, Expression value, List<Expression> list) {
        super(source, value, list);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.In> info() {
        return NodeInfo.create(this, In::new, value(), list());
    }

    @Override
    public In replaceChildren(List<Expression> newChildren) {
        return new In(source(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    @Override
    public boolean foldable() {
        // QL's In fold()s to null, if value() is null, but isn't foldable() unless all children are
        // TODO: update this null check in QL too?
        return Expressions.isNull(value()) || super.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator) {
        var commonType = commonType();
        EvalOperator.ExpressionEvaluator.Factory lhs;
        EvalOperator.ExpressionEvaluator.Factory[] factories;
        if (commonType.isNumeric()) {
            lhs = Cast.cast(source(), value().dataType(), commonType, toEvaluator.apply(value()));
            factories = list().stream()
                .map(e -> Cast.cast(source(), e.dataType(), commonType, toEvaluator.apply(e)))
                .toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
        } else {
            lhs = toEvaluator.apply(value());
            factories = list().stream()
                .map(e -> toEvaluator.apply(e))
                .toArray(EvalOperator.ExpressionEvaluator.Factory[]::new);
        }

        if (commonType == DataTypes.BOOLEAN) {
            return new InBooleanEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == DataTypes.DOUBLE) {
            return new InDoubleEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == DataTypes.INTEGER) {
            return new InIntEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == DataTypes.LONG || commonType == DataTypes.DATETIME || commonType == DataTypes.UNSIGNED_LONG ) {
            return new InLongEvaluator.Factory(source(), lhs, factories);
        }
        if (commonType == DataTypes.KEYWORD
            || commonType == DataTypes.TEXT
            || commonType == DataTypes.IP
            || commonType == DataTypes.VERSION
            || commonType == DataTypes.UNSUPPORTED) {
            return new InBytesRefEvaluator.Factory(source(), toEvaluator.apply(value()), factories);
        }
        if (commonType == DataTypes.NULL) {
            return EvalOperator.CONSTANT_NULL_FACTORY;
        }
        throw EsqlIllegalArgumentException.illegalDataType(commonType);
    }

    private DataType commonType() {
        DataType commonType = value().dataType();
        for (Expression e : list()) {
            if (e.dataType() == DataTypes.NULL && value().dataType() != DataTypes.NULL) {
                continue;
            }
            commonType = EsqlDataTypeRegistry.INSTANCE.commonType(commonType, e.dataType());
        }
        return commonType;
    }

    @Override
    protected boolean areCompatible(DataType left, DataType right) {
        if (left == DataTypes.UNSIGNED_LONG || right == DataTypes.UNSIGNED_LONG) {
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

    @Evaluator(extraName = "Boolean")
    static boolean process(boolean lhs, boolean[] rhs) {
        Boolean result = Boolean.FALSE;
        for (Object v : rhs) {
            Boolean compResult = Comparisons.eq(lhs, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    @Evaluator(extraName = "BytesRef")
    static boolean process(BytesRef lhs, BytesRef[] rhs) {
        Boolean result = Boolean.FALSE;
        for (Object v : rhs) {
            Boolean compResult = Comparisons.eq(lhs, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    @Evaluator(extraName = "Int")
    static boolean process(int lhs, int[] rhs) {
        Boolean result = Boolean.FALSE;
        for (Object v : rhs) {
            Boolean compResult = Comparisons.eq(lhs, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    @Evaluator(extraName = "Long")
    static boolean process(long lhs, long[] rhs) {
        Boolean result = Boolean.FALSE;
        for (Object v : rhs) {
            Boolean compResult = Comparisons.eq(lhs, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    @Evaluator(extraName = "Double")
    static boolean process(double lhs, double[] rhs) {
        Boolean result = Boolean.FALSE;
        for (Object v : rhs) {
            Boolean compResult = Comparisons.eq(lhs, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }
}
