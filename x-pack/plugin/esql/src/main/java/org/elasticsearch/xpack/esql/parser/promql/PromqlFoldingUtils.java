/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.Arithmetics;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.arithmetic.VectorBinaryArithmetic.ArithmeticOp;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.comparison.VectorBinaryComparison.ComparisonOp;

import java.time.Duration;

/**
 * Utility class for evaluating scalar arithmetic operations at parse time.
 * Handles operations between:
 * - Numbers (delegates to Arithmetics)
 * - Durations and numbers (converts to seconds, computes, converts back)
 * - Durations and durations (only for ADD/SUB)
 */
public class PromqlFoldingUtils {

    /**
     * Evaluate arithmetic operation between two scalar values at parse time.
     *
     * @param source Source location for error messages
     * @param left Left operand (Number or Duration)
     * @param right Right operand (Number or Duration)
     * @param operation The arithmetic operation
     * @return Result value (Number or Duration)
     */
    public static Object evaluate(Source source, Object left, Object right, ArithmeticOp operation) {
        // Dispatch to appropriate handler based on operand types
        if (left instanceof Duration leftDuration) {
            if (right instanceof Duration rightDuration) {
                return arithmetics(source, leftDuration, rightDuration, operation);
            } else if (right instanceof Number rightNumber) {
                return arithmetics(source, leftDuration, rightNumber, operation);
            }
        } else if (left instanceof Number leftNumber) {
            if (right instanceof Duration rightDuration) {
                return arithmetics(source, leftNumber, rightDuration, operation);
            } else if (right instanceof Number rightNumber) {
                return numericArithmetics(source, leftNumber, rightNumber, operation);
            }
        }

        throw new ParsingException(
            source,
            "Cannot perform arithmetic between [{}] and [{}]",
            left.getClass().getSimpleName(),
            right.getClass().getSimpleName()
        );
    }

    /**
     * Duration op Duration (only ADD and SUB supported).
     */
    private static Duration arithmetics(Source source, Duration left, Duration right, ArithmeticOp op) {
        Duration result = switch (op) {
            case ADD -> left.plus(right);
            case SUB -> left.minus(right);
            default -> throw new ParsingException(source, "Operation [{}] not supported between two durations", op);
        };

        return result;
    }

    /**
     * Duration op Number.
     * For ADD/SUB: Number interpreted as seconds (PromQL convention).
     * For MUL/DIV/MOD/POW: Number is a dimensionless scalar.
     */
    private static Duration arithmetics(Source source, Duration duration, Number scalar, ArithmeticOp op) {
        long durationSeconds = duration.getSeconds();
        long scalarValue = scalar.longValue();

        long resultSeconds = switch (op) {
            case ADD -> {
                yield Math.addExact(durationSeconds, scalarValue);
            }
            case SUB -> {
                yield Math.subtractExact(durationSeconds, scalarValue);
            }
            case MUL -> {
                yield Math.round(durationSeconds * scalar.doubleValue());
            }
            case DIV -> {
                if (scalarValue == 0) {
                    throw new ParsingException(source, "Cannot divide duration by zero");
                }
                yield Math.round(durationSeconds / scalar.doubleValue());
            }
            case MOD -> {
                // Modulo operation
                if (scalarValue == 0) {
                    throw new ParsingException(source, "Cannot compute modulo with zero");
                }
                yield Math.floorMod(durationSeconds, scalarValue);
            }
            case POW -> {
                // Power operation (duration ^ scalar)
                yield Math.round(Math.pow(durationSeconds, scalarValue));
            }
        };

        return Duration.ofSeconds(resultSeconds);
    }

    private static Duration arithmetics(Source source, Number scalar, Duration duration, ArithmeticOp op) {
        return switch (op) {
            case ADD -> arithmetics(source, duration, scalar, ArithmeticOp.ADD);
            case SUB -> arithmetics(source, Duration.ofSeconds(scalar.longValue()), duration, ArithmeticOp.SUB);
            case MUL -> arithmetics(source, duration, scalar, ArithmeticOp.MUL);
            default -> throw new ParsingException(source, "Operation [{}] not supported with scalar on left and duration on right", op);
        };
    }

    /**
     * Number op Number (pure numeric operations).
     * Delegates to Arithmetics for consistent numeric handling.
     */
    private static Number numericArithmetics(Source source, Number left, Number right, ArithmeticOp op) {
        try {
            return switch (op) {
                case ADD -> Arithmetics.add(left, right);
                case SUB -> Arithmetics.sub(left, right);
                case MUL -> Arithmetics.mul(left, right);
                case DIV -> Arithmetics.div(left, right);
                case MOD -> Arithmetics.mod(left, right);
                case POW -> {
                    // Power not in Arithmetics, compute manually
                    double result = Math.pow(left.doubleValue(), right.doubleValue());
                    // Try to preserve integer types when possible
                    if (Double.isFinite(result)) {
                        if (result == (long) result) {
                            if (result >= Integer.MIN_VALUE && result <= Integer.MAX_VALUE) {
                                yield (int) result;
                            }
                            yield (long) result;
                        }
                    }
                    yield result;
                }
            };
        } catch (ArithmeticException e) {
            throw new ParsingException(source, "Arithmetic error: {}", e.getMessage());
        }
    }

    /**
     * Evaluate comparison operation between two numbers at parse time.
     *
     * @param left Left operand (Number)
     * @param right Right operand (Number)
     * @param operation The comparison operation
     * @return true if comparison holds, false otherwise
     */
    public static boolean evaluate(Source source, Object left, Object right, ComparisonOp operation) {
        if (left instanceof Number ln && right instanceof Number rn) {
            // Get double values once, reuse for comparison - avoids extra allocation
            double l = ln.doubleValue();
            double r = rn.doubleValue();

            return switch (operation) {
                case EQ -> l == r;
                case NEQ -> l != r;
                case GT -> l > r;
                case GTE -> l >= r;
                case LT -> l < r;
                case LTE -> l <= r;
            };
        }
        throw new ParsingException(
            source,
            "Cannot perform comparison between [{}] and [{}]",
            left.getClass().getSimpleName(),
            right.getClass().getSimpleName()
        );
    }

    /**
     * Validate that duration is positive (PromQL requirement).
     */
    private static void validatePositiveDuration(Source source, Duration duration) {
        if (duration.isNegative() || duration.isZero()) {
            throw new ParsingException(source, "Duration must be positive, got [{}]", duration);
        }
    }
}
