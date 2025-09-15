/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.time.DateTimeException;
import java.util.List;
import java.util.function.BiFunction;

import static java.lang.Math.signum;
import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.ADD;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.DIV;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.MOD;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.MUL;
import static org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.DefaultBinaryArithmeticOperation.SUB;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

/**
 * Simplifies arithmetic expressions with BinaryComparisons and fixed point fields, such as: (int + 2) / 3 > 4 => int > 10
 */
public final class SimplifyComparisonsArithmetics extends OptimizerRules.OptimizerExpressionRule<BinaryComparison> {
    BiFunction<DataType, DataType, Boolean> typesCompatible;

    public SimplifyComparisonsArithmetics(BiFunction<DataType, DataType, Boolean> typesCompatible) {
        super(OptimizerRules.TransformDirection.UP);
        this.typesCompatible = typesCompatible;
    }

    @Override
    protected Expression rule(BinaryComparison bc, LogicalOptimizerContext ctx) {
        // optimize only once the expression has a literal on the right side of the binary comparison
        if (bc.right() instanceof Literal) {
            if (bc.left() instanceof ArithmeticOperation) {
                return simplifyBinaryComparison(ctx.foldCtx(), bc);
            }
            if (bc.left() instanceof Neg) {
                return foldNegation(ctx.foldCtx(), bc);
            }
        }
        return bc;
    }

    private Expression simplifyBinaryComparison(FoldContext foldContext, BinaryComparison comparison) {
        ArithmeticOperation operation = (ArithmeticOperation) comparison.left();
        // Use symbol comp: SQL operations aren't available in this package (as dependencies)
        String opSymbol = operation.symbol();
        // Modulo can't be simplified.
        if (opSymbol.equals(MOD.symbol())) {
            return comparison;
        }
        OperationSimplifier simplification = null;
        if (isMulOrDiv(opSymbol)) {
            simplification = new MulDivSimplifier(foldContext, comparison);
        } else if (opSymbol.equals(ADD.symbol()) || opSymbol.equals(SUB.symbol())) {
            simplification = new AddSubSimplifier(foldContext, comparison);
        }

        return (simplification == null || simplification.isUnsafe(typesCompatible)) ? comparison : simplification.apply();
    }

    private static boolean isMulOrDiv(String opSymbol) {
        return opSymbol.equals(MUL.symbol()) || opSymbol.equals(DIV.symbol());
    }

    private static Expression foldNegation(FoldContext ctx, BinaryComparison bc) {
        Literal bcLiteral = (Literal) bc.right();
        Expression literalNeg = tryFolding(ctx, new Neg(bcLiteral.source(), bcLiteral));
        return literalNeg == null ? bc : bc.reverse().replaceChildren(asList(((Neg) bc.left()).field(), literalNeg));
    }

    private static Expression tryFolding(FoldContext ctx, Expression expression) {
        if (expression.foldable()) {
            try {
                expression = new Literal(expression.source(), expression.fold(ctx), expression.dataType());
            } catch (ArithmeticException | DateTimeException e) {
                // null signals that folding would result in an over-/underflow (such as Long.MAX_VALUE+1); the optimisation is skipped.
                expression = null;
            }
        }
        return expression;
    }

    private abstract static class OperationSimplifier {
        final FoldContext foldContext;
        final BinaryComparison comparison;
        final Literal bcLiteral;
        final ArithmeticOperation operation;
        final Expression opLeft;
        final Expression opRight;
        final Literal opLiteral;

        OperationSimplifier(FoldContext foldContext, BinaryComparison comparison) {
            this.foldContext = foldContext;
            this.comparison = comparison;
            operation = (ArithmeticOperation) comparison.left();
            bcLiteral = (Literal) comparison.right();

            opLeft = operation.left();
            opRight = operation.right();

            if (opLeft instanceof Literal) {
                opLiteral = (Literal) opLeft;
            } else if (opRight instanceof Literal) {
                opLiteral = (Literal) opRight;
            } else {
                opLiteral = null;
            }
        }

        // can it be quickly fast-tracked that the operation can't be reduced?
        final boolean isUnsafe(BiFunction<DataType, DataType, Boolean> typesCompatible) {
            if (opLiteral == null) {
                // one of the arithm. operands must be a literal, otherwise the operation wouldn't simplify anything
                return true;
            }

            // Only operations on fixed point literals are supported, since optimizing float point operations can also change the
            // outcome of the filtering:
            // x + 1e18 > 1e18::long will yield different results with a field value in [-2^6, 2^6], optimised vs original;
            // x * (1 + 1e-15d) > 1 : same with a field value of (1 - 1e-15d)
            // so consequently, int fields optimisation requiring FP arithmetic isn't possible either: (x - 1e-15) * (1 + 1e-15) > 1.
            if (opLiteral.dataType().isRationalNumber() || bcLiteral.dataType().isRationalNumber()) {
                return true;
            }

            // the Literal will be moved to the right of the comparison, but only if data-compatible with what's there
            if (typesCompatible.apply(bcLiteral.dataType(), opLiteral.dataType()) == false) {
                return true;
            }

            return isOpUnsafe();
        }

        final Expression apply() {
            // force float point folding for FlP field
            Literal bcl = operation.dataType().isRationalNumber()
                ? new Literal(bcLiteral.source(), ((Number) bcLiteral.value()).doubleValue(), DataType.DOUBLE)
                : bcLiteral;

            Expression bcRightExpression = ((BinaryComparisonInversible) operation).binaryComparisonInverse()
                .create(bcl.source(), bcl, opRight);
            bcRightExpression = tryFolding(foldContext, bcRightExpression);
            return bcRightExpression != null
                ? postProcess((BinaryComparison) comparison.replaceChildren(List.of(opLeft, bcRightExpression)))
                : comparison;
        }

        // operation-specific operations:
        // - fast-tracking of simplification unsafety
        abstract boolean isOpUnsafe();

        // - post optimisation adjustments
        Expression postProcess(BinaryComparison binaryComparison) {
            return binaryComparison;
        }
    }

    private static class AddSubSimplifier extends OperationSimplifier {

        AddSubSimplifier(FoldContext foldContext, BinaryComparison comparison) {
            super(foldContext, comparison);
        }

        @Override
        boolean isOpUnsafe() {
            // no ADD/SUB with floating fields
            if (operation.dataType().isRationalNumber()) {
                return true;
            }

            if (operation.symbol().equals(SUB.symbol()) && opRight instanceof Literal == false) { // such as: 1 - x > -MAX
                // if next simplification step would fail on overflow anyways, skip the optimisation already
                return tryFolding(foldContext, new Sub(EMPTY, opLeft, bcLiteral)) == null;
            }

            return false;
        }
    }

    private static class MulDivSimplifier extends OperationSimplifier {

        private final boolean isDiv; // and not MUL.
        private final int opRightSign; // sign of the right operand in: (left) (op) (right) (comp) (literal)

        MulDivSimplifier(FoldContext foldContext, BinaryComparison comparison) {
            super(foldContext, comparison);
            isDiv = operation.symbol().equals(DIV.symbol());
            opRightSign = sign(opRight);
        }

        @Override
        boolean isOpUnsafe() {
            // Integer divisions are not safe to optimise: x / 5 > 1 <=/=> x > 5 for x in [6, 9]; same for the `==` comp
            if (operation.dataType().isWholeNumber() && isDiv) {
                return true;
            }

            // If current operation is a multiplication, it's inverse will be a division: safe only if outcome is still integral.
            if (isDiv == false && opLeft.dataType().isWholeNumber()) {
                long opLiteralValue = ((Number) opLiteral.value()).longValue();
                return opLiteralValue == 0 || ((Number) bcLiteral.value()).longValue() % opLiteralValue != 0;
            }

            // can't move a 0 in Mul/Div comparisons
            return opRightSign == 0;
        }

        @Override
        Expression postProcess(BinaryComparison binaryComparison) {
            // negative multiplication/division changes the direction of the comparison
            return opRightSign < 0 ? binaryComparison.reverse() : binaryComparison;
        }

        private static int sign(Object obj) {
            int sign = 1;
            if (obj instanceof Number) {
                sign = (int) signum(((Number) obj).doubleValue());
            } else if (obj instanceof Literal) {
                sign = sign(((Literal) obj).value());
            } else if (obj instanceof Neg) {
                sign = -sign(((Neg) obj).field());
            } else if (obj instanceof ArithmeticOperation operation) {
                if (isMulOrDiv(operation.symbol())) {
                    sign = sign(operation.left()) * sign(operation.right());
                }
            }
            return sign;
        }
    }
}
