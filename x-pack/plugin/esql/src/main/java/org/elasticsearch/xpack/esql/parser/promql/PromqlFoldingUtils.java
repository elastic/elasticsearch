/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryArithmetic.ArithmeticOp;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison.ComparisonOp;

import java.time.Duration;

/**
 * Folds scalar PromQL arithmetic and comparisons at parse time.
 * <p>
 * Supported operands:
 * - number op number: evaluated as double
 * - duration op duration: only addition and subtraction
 * - duration op number: duration is converted to seconds, evaluated, then converted back
 * - number op duration: only supported where PromQL duration semantics are well-defined
 */
public abstract class PromqlFoldingUtils {
    private PromqlFoldingUtils() {}

    public static Object evaluate(Source source, Object left, Object right, ArithmeticOp op) {
        if (left instanceof Number l && right instanceof Number r) {
            return arithmetic(l, r, op);
        }
        if (left instanceof Duration l && right instanceof Duration r) {
            return arithmetic(source, l, r, op);
        }
        if (left instanceof Duration l && right instanceof Number r) {
            return arithmetic(source, l, r, op);
        }
        if (left instanceof Number l && right instanceof Duration r) {
            return arithmetic(source, l, r, op);
        }

        throw cannotApply(source, op, left, right);
    }

    public static boolean evaluate(Source source, Object left, Object right, ComparisonOp op) {
        if (left instanceof Number ln && right instanceof Number rn) {
            double l = ln.doubleValue();
            double r = rn.doubleValue();

            return switch (op) {
                case EQ -> l == r;
                case NEQ -> l != r;
                case GT -> l > r;
                case GTE -> l >= r;
                case LT -> l < r;
                case LTE -> l <= r;
            };
        }

        throw cannotApply(source, op, left, right);
    }

    private static double arithmetic(Number left, Number right, ArithmeticOp op) {
        double l = left.doubleValue();
        double r = right.doubleValue();

        return switch (op) {
            case ADD -> l + r;
            case SUB -> l - r;
            case MUL -> l * r;
            case DIV -> l / r;
            case MOD -> l % r;
            case POW -> Math.pow(l, r);
        };
    }

    private static Duration arithmetic(Source source, Duration left, Duration right, ArithmeticOp op) {
        return switch (op) {
            case ADD -> left.plus(right);
            case SUB -> left.minus(right);
            default -> throw cannotApply(source, op, left, right);
        };
    }

    private static Duration arithmetic(Source source, Duration duration, Number scalar, ArithmeticOp op) {
        long seconds = duration.getSeconds();
        long scalarSeconds = scalar.longValue();
        double scalarValue = scalar.doubleValue();

        long result = switch (op) {
            case ADD -> Math.addExact(seconds, scalarSeconds);
            case SUB -> Math.subtractExact(seconds, scalarSeconds);
            case MUL -> Math.round(seconds * scalarValue);
            case DIV -> {
                if (scalarValue == 0d) {
                    throw new ParsingException(source, "division of a duration by zero is not allowed");
                }
                yield Math.round(seconds / scalarValue);
            }
            case MOD -> {
                if (scalarSeconds == 0L) {
                    throw new ParsingException(source, "modulo of a duration by zero is not allowed");
                }
                yield Math.floorMod(seconds, scalarSeconds);
            }
            case POW -> Math.round(Math.pow(seconds, scalarValue));
        };

        return Duration.ofSeconds(result);
    }

    private static Duration arithmetic(Source source, Number scalar, Duration duration, ArithmeticOp op) {
        return switch (op) {
            case ADD -> arithmetic(source, duration, scalar, ArithmeticOp.ADD);
            case SUB -> Duration.ofSeconds(Math.subtractExact(scalar.longValue(), duration.getSeconds()));
            case MUL -> arithmetic(source, duration, scalar, ArithmeticOp.MUL);
            default -> throw cannotApply(source, op, scalar, duration);
        };
    }

    private static String symbol(ArithmeticOp op) {
        return switch (op) {
            case ADD -> "+";
            case SUB -> "-";
            case MUL -> "*";
            case DIV -> "/";
            case MOD -> "%";
            case POW -> "^";
        };
    }

    private static String typeName(Object value) {
        return value instanceof Duration ? "duration" : "scalar";
    }

    private static ParsingException cannotApply(Source source, ArithmeticOp op, Object left, Object right) {
        return new ParsingException(source, "operator [{}] is not defined for {} and {}", symbol(op), typeName(left), typeName(right));
    }

    private static ParsingException cannotApply(Source source, ComparisonOp op, Object left, Object right) {
        return new ParsingException(
            source,
            "comparison operator [{}] requires scalar operands, got {} and {}",
            op.toString().toLowerCase(java.util.Locale.ROOT),
            typeName(left),
            typeName(right)
        );
    }
}
