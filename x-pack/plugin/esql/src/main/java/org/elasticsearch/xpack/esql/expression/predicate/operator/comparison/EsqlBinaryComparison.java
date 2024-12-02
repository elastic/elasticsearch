/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

public abstract class EsqlBinaryComparison extends BinaryComparison implements EvaluatorMapper {

    private final Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap;

    private final BinaryComparisonOperation functionType;

    @FunctionalInterface
    public interface BinaryOperatorConstructor {
        EsqlBinaryComparison apply(Source source, Expression lhs, Expression rhs);
    }

    public enum BinaryComparisonOperation implements Writeable {

        EQ(0, "==", org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.EQ, Equals::new),
        // id 1 reserved for NullEquals
        NEQ(
            2,
            "!=",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.NEQ,
            NotEquals::new
        ),
        GT(
            3,
            ">",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.GT,
            GreaterThan::new
        ),
        GTE(
            4,
            ">=",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.GTE,
            GreaterThanOrEqual::new
        ),
        LT(5, "<", org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.LT, LessThan::new),
        LTE(
            6,
            "<=",
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation.LTE,
            LessThanOrEqual::new
        );

        private final int id;
        private final String symbol;
        // Temporary mapping to the old enum, to satisfy the superclass constructor signature.
        private final org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation shim;
        private final BinaryOperatorConstructor constructor;

        BinaryComparisonOperation(
            int id,
            String symbol,
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparisonOperation shim,
            BinaryOperatorConstructor constructor
        ) {
            this.id = id;
            this.symbol = symbol;
            this.shim = shim;
            this.constructor = constructor;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(id);
        }

        public static BinaryComparisonOperation readFromStream(StreamInput in) throws IOException {
            int id = in.readVInt();
            for (BinaryComparisonOperation op : values()) {
                if (op.id == id) {
                    return op;
                }
            }
            throw new IOException("No BinaryComparisonOperation found for id [" + id + "]");
        }

        public String symbol() {
            return symbol;
        }

        public EsqlBinaryComparison buildNewInstance(Source source, Expression lhs, Expression rhs) {
            return constructor.apply(source, lhs, rhs);
        }
    }

    protected EsqlBinaryComparison(
        Source source,
        Expression left,
        Expression right,
        BinaryComparisonOperation operation,
        Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap
    ) {
        this(source, left, right, operation, null, evaluatorMap);
    }

    protected EsqlBinaryComparison(
        Source source,
        Expression left,
        Expression right,
        BinaryComparisonOperation operation,
        // TODO: We are definitely not doing the right thing with this zoneId
        ZoneId zoneId,
        Map<DataType, EsqlArithmeticOperation.BinaryEvaluator> evaluatorMap
    ) {
        super(source, left, right, operation.shim, zoneId);
        this.evaluatorMap = evaluatorMap;
        this.functionType = operation;
    }

    public static EsqlBinaryComparison readFrom(StreamInput in) throws IOException {
        // TODO this uses a constructor on the operation *and* a name which is confusing. It only needs one. Everything else uses a name.
        var source = Source.readFrom((PlanStreamInput) in);
        EsqlBinaryComparison.BinaryComparisonOperation operation = EsqlBinaryComparison.BinaryComparisonOperation.readFromStream(in);
        var left = in.readNamedWriteable(Expression.class);
        var right = in.readNamedWriteable(Expression.class);
        // TODO: Remove zoneId entirely
        var zoneId = in.readOptionalZoneId();
        return operation.buildNewInstance(source, left, right);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        functionType.writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
        out.writeOptionalZoneId(zoneId());
    }

    public BinaryComparisonOperation getFunctionType() {
        return functionType;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // Our type is always boolean, so figure out the evaluator type from the inputs
        DataType commonType = commonType(left().dataType(), right().dataType());
        EvalOperator.ExpressionEvaluator.Factory lhs;
        EvalOperator.ExpressionEvaluator.Factory rhs;

        if (commonType.isNumeric()) {
            lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
            rhs = Cast.cast(source(), right().dataType(), commonType, toEvaluator.apply(right()));
        } else {
            lhs = toEvaluator.apply(left());
            rhs = toEvaluator.apply(right());
        }

        if (evaluatorMap.containsKey(commonType) == false) {
            throw new EsqlIllegalArgumentException("Unsupported type " + left().dataType());
        }
        return evaluatorMap.get(commonType).apply(source(), lhs, rhs);
    }

    @Override
    public Boolean fold() {
        return (Boolean) EvaluatorMapper.super.fold();
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution typeResolution = super.resolveType();
        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        return checkCompatibility();
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return TypeResolutions.isType(
            e,
            evaluatorMap::containsKey,
            sourceText(),
            paramOrdinal,
            evaluatorMap.keySet().stream().map(DataType::typeName).sorted().toArray(String[]::new)
        );
    }

    /**
     * Check if the two input types are compatible for this operation
     *
     * @return TypeResolution.TYPE_RESOLVED iff the types are compatible.  Otherwise, an appropriate type resolution error.
     */
    protected TypeResolution checkCompatibility() {
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();

        // Unsigned long is only interoperable with other unsigned longs
        if ((rightType == UNSIGNED_LONG && (false == (leftType == UNSIGNED_LONG || leftType == DataType.NULL)))
            || (leftType == UNSIGNED_LONG && (false == (rightType == UNSIGNED_LONG || rightType == DataType.NULL)))) {
            return new TypeResolution(formatIncompatibleTypesMessage());
        }

        if ((leftType.isNumeric() && rightType.isNumeric())
            || (DataType.isString(leftType) && DataType.isString(rightType))
            || leftType.equals(rightType)
            || DataType.isNull(leftType)
            || DataType.isNull(rightType)) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return new TypeResolution(formatIncompatibleTypesMessage());
    }

    public String formatIncompatibleTypesMessage() {
        if (left().dataType().equals(UNSIGNED_LONG)) {
            return format(
                null,
                "first argument of [{}] is [unsigned_long] and second is [{}]. "
                    + "[unsigned_long] can only be operated on together with another [unsigned_long]",
                sourceText(),
                right().dataType().typeName()
            );
        }
        if (right().dataType().equals(UNSIGNED_LONG)) {
            return format(
                null,
                "first argument of [{}] is [{}] and second is [unsigned_long]. "
                    + "[unsigned_long] can only be operated on together with another [unsigned_long]",
                sourceText(),
                left().dataType().typeName()
            );
        }
        return format(
            null,
            "first argument of [{}] is [{}] so second argument must also be [{}] but was [{}]",
            sourceText(),
            left().dataType().isNumeric() ? "numeric" : left().dataType().typeName(),
            left().dataType().isNumeric() ? "numeric" : left().dataType().typeName(),
            right().dataType().typeName()
        );
    }

}
