/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.BinaryArithmeticOperation;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

public abstract class EsqlArithmeticOperation extends ArithmeticOperation implements EvaluatorMapper {

    /**
     * The only role of this enum is to fit the super constructor that expects a BinaryOperation which is
     * used just for its symbol.
     * The rest of the methods should not be triggered hence the UOE.
     */
    enum OperationSymbol implements BinaryArithmeticOperation {
        ADD("+"),
        SUB("-"),
        MUL("*"),
        DIV("/"),
        MOD("%");

        private final String symbol;

        OperationSymbol(String symbol) {
            this.symbol = symbol;
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object doApply(Object o, Object o2) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String symbol() {
            return symbol;
        }
    }

    /** Arithmetic (quad) function. */
    @FunctionalInterface
    public interface BinaryEvaluator {
        ExpressionEvaluator.Factory apply(Source source, ExpressionEvaluator.Factory lhs, ExpressionEvaluator.Factory rhs);
    }

    /** Constant-RHS fast-path factory for INTEGER common type. */
    @FunctionalInterface
    public interface IntConstantFactory {
        ExpressionEvaluator.Factory create(Source source, ExpressionEvaluator.Factory lhs, int rhs);
    }

    /** Constant-RHS fast-path factory for LONG common type. */
    @FunctionalInterface
    public interface LongConstantFactory {
        ExpressionEvaluator.Factory create(Source source, ExpressionEvaluator.Factory lhs, long rhs);
    }

    /** Constant-RHS fast-path factory for DOUBLE common type. */
    @FunctionalInterface
    public interface DoubleConstantFactory {
        ExpressionEvaluator.Factory create(Source source, ExpressionEvaluator.Factory lhs, double rhs);
    }

    private final BinaryEvaluator ints;
    private final BinaryEvaluator longs;
    private final BinaryEvaluator ulongs;
    private final BinaryEvaluator doubles;

    // Constant-RHS fast-path factories. All null for operators with no fast path.
    private final IntConstantFactory intsConst;
    private final LongConstantFactory longsConst;
    private final DoubleConstantFactory doublesConst;
    // When true, the fast path is skipped for a foldable-zero RHS so the binary
    // path's per-row /-by-zero warning contract is preserved (Mod, Div).
    private final boolean excludeZeroRhs;

    private DataType dataType;

    EsqlArithmeticOperation(
        Source source,
        Expression left,
        Expression right,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles
    ) {
        this(source, left, right, op, ints, longs, ulongs, doubles, null, null, null, false);
    }

    EsqlArithmeticOperation(
        Source source,
        Expression left,
        Expression right,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles,
        IntConstantFactory intsConst,
        LongConstantFactory longsConst,
        DoubleConstantFactory doublesConst,
        boolean excludeZeroRhs
    ) {
        super(source, left, right, op);
        this.ints = ints;
        this.longs = longs;
        this.ulongs = ulongs;
        this.doubles = doubles;
        this.intsConst = intsConst;
        this.longsConst = longsConst;
        this.doublesConst = doublesConst;
        this.excludeZeroRhs = excludeZeroRhs;
    }

    EsqlArithmeticOperation(
        StreamInput in,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles
    ) throws IOException {
        this(in, op, ints, longs, ulongs, doubles, null, null, null, false);
    }

    EsqlArithmeticOperation(
        StreamInput in,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles,
        IntConstantFactory intsConst,
        LongConstantFactory longsConst,
        DoubleConstantFactory doublesConst,
        boolean excludeZeroRhs
    ) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            op,
            ints,
            longs,
            ulongs,
            doubles,
            intsConst,
            longsConst,
            doublesConst,
            excludeZeroRhs
        );
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    public DataType dataType() {
        if (dataType == null) {
            dataType = commonType(left().dataType(), right().dataType());
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution typeResolution = super.resolveType();
        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        return checkCompatibility();
    }

    /**
     * Check if the two input types are compatible for this operation
     *
     * @return TypeResolution.TYPE_RESOLVED iff the types are compatible.  Otherwise, an appropriate type resolution error.
     */
    protected TypeResolution checkCompatibility() {
        // This checks that unsigned longs should only be compatible with other unsigned longs
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();
        if ((rightType == UNSIGNED_LONG && (false == (leftType == UNSIGNED_LONG || leftType == DataType.NULL)))
            || (leftType == UNSIGNED_LONG && (false == (rightType == UNSIGNED_LONG || rightType == DataType.NULL)))) {
            return new TypeResolution(formatIncompatibleTypesMessage(symbol(), leftType, rightType));
        }

        // at this point, left should be null, and right should be null or numeric.
        return TypeResolution.TYPE_RESOLVED;
    }

    public static String formatIncompatibleTypesMessage(String symbol, DataType leftType, DataType rightType) {
        return format(null, "[{}] has arguments with incompatible types [{}] and [{}]", symbol, leftType.typeName(), rightType.typeName());
    }

    /**
     * Dispatch: try the constant-RHS fast path first (if the operator opted in via
     * the constructor's fast-path factories); fall through to the binary path.
     * Subclasses can still override this for orthogonal concerns (dense-vector,
     * date-time) and call {@code super.toEvaluator(...)} for the numeric cases.
     */
    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory fast = tryConstantRhsFastPath(toEvaluator);
        return fast != null ? fast : binaryToEvaluator(toEvaluator);
    }

    private ExpressionEvaluator.Factory tryConstantRhsFastPath(ToEvaluator toEvaluator) {
        if (intsConst == null && longsConst == null && doublesConst == null) {
            return null;
        }
        Expression right = right();
        if (right.foldable() == false) {
            return null;
        }
        Object folded = right.fold(toEvaluator.foldCtx());
        if (folded instanceof Number == false) {
            return null;
        }
        Number rhs = (Number) folded;
        DataType commonType = dataType();
        if (excludeZeroRhs && isZero(rhs, commonType)) {
            return null;
        }
        if (commonType == INTEGER && intsConst != null) {
            ExpressionEvaluator.Factory lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
            return intsConst.create(source(), lhs, rhs.intValue());
        }
        if (commonType == LONG && longsConst != null) {
            ExpressionEvaluator.Factory lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
            return longsConst.create(source(), lhs, rhs.longValue());
        }
        if (commonType == DOUBLE && doublesConst != null) {
            ExpressionEvaluator.Factory lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
            return doublesConst.create(source(), lhs, rhs.doubleValue());
        }
        // UNSIGNED_LONG is the intentional binary-path fallback for operators that opt into
        // the fast path (we have no UNSIGNED_LONG constant-RHS evaluator yet). Anything else
        // is unclassified — likely a new numeric type added without extending the fast path.
        if (commonType == UNSIGNED_LONG) {
            return null;
        }
        throw new EsqlIllegalArgumentException(
            "constant-RHS fast path: type " + commonType + " is unclassified on " + getClass().getSimpleName()
        );
    }

    private ExpressionEvaluator.Factory binaryToEvaluator(ToEvaluator toEvaluator) {
        var commonType = dataType();
        var leftType = left().dataType();
        if (leftType.isNumeric()) {

            var lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
            var rhs = Cast.cast(source(), right().dataType(), commonType, toEvaluator.apply(right()));

            BinaryEvaluator eval;
            if (commonType == INTEGER) {
                eval = ints;
            } else if (commonType == LONG) {
                eval = longs;
            } else if (commonType == UNSIGNED_LONG) {
                eval = ulongs;
            } else if (commonType == DOUBLE) {
                eval = doubles;
            } else {
                throw new EsqlIllegalArgumentException("Unsupported type " + commonType);
            }
            return eval.apply(source(), lhs, rhs);
        }
        throw new EsqlIllegalArgumentException("Unsupported type " + leftType);
    }

    private static boolean isZero(Number value, DataType type) {
        if (type == INTEGER) {
            return value.intValue() == 0;
        }
        if (type == LONG) {
            return value.longValue() == 0L;
        }
        if (type == DOUBLE) {
            return value.doubleValue() == 0.0d;
        }
        return false;
    }
}
