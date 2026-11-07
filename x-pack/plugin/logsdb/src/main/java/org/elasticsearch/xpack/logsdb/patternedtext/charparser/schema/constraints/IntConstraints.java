/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.OperatorType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.PatternUtils;

public class IntConstraints {

    /**
     * Represents an inclusive range of integers with a lower and upper bound.
     */
    public record Range(int lowerBound, int upperBound) {}

    /**
     * Parses a raw constraint string from schema.yaml and returns an {@link IntConstraint}
     * that implements the constraint.
     * Handles:
     * <ul>
     *     <li>Simple constraints: {@code 0-255}, {@code >=0}, {@code ==7}, etc.</li>
     *     <li>Logical operators: {@code &&} (AND), {@code ||} (OR)</li>
     *     <li>Negative numbers: {@code (-128)} for negative values</li>
     * </ul>
     *
     * @param rawConstraint the constraint string as defined in schema.yaml
     * @return an IntConstraint that evaluates the constraint
     * @throws IllegalArgumentException if the constraint format is invalid
     */
    public static IntConstraint parseIntConstraint(String rawConstraint) {
        if (rawConstraint == null || rawConstraint.trim().isEmpty()) {
            return AnyInteger.INSTANCE;
        }

        String constraint = rawConstraint.trim();

        // Handle logical operators first
        String[] chainedConstraints = PatternUtils.splitChainedAndConstraints(constraint);
        if (chainedConstraints.length == 2) {
            return parseIntConstraint(chainedConstraints[0]).and(parseIntConstraint(chainedConstraints[1]));
        }
        chainedConstraints = PatternUtils.splitChainedOrConstraints(constraint);
        if (chainedConstraints.length == 2) {
            return parseIntConstraint(chainedConstraints[0]).or(parseIntConstraint(chainedConstraints[1]));
        }

        // Handle simple constraints
        PatternUtils.ConstraintDetails details = PatternUtils.parseSimpleConstraint(constraint);
        String[] stringOperands = details.operands();
        int[] intOperands = new int[stringOperands.length];

        // Convert string operands to integers
        for (int i = 0; i < intOperands.length; i++) {
            try {
                intOperands[i] = Integer.parseInt(stringOperands[i]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid numeric value in constraint: " + stringOperands[i]);
            }
        }

        // Create and return the appropriate IntPredicate
        return createConstraint(details.operator(), intOperands);
    }

    /**
     * Creates an {@link IntConstraint} based on the given operator and operands.
     *
     * @param operator the operator type from OperatorType enum
     * @param operands the integer operands for the constraint
     * @return an IntConstraint that evaluates the constraint
     * @throws IllegalArgumentException if the operator is unsupported or operands are invalid
     */
    public static IntConstraint createConstraint(OperatorType operator, int... operands) {
        return switch (operator) {
            case EQUALITY -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new EqualsIntConstraint(operands[0]);
            }
            case LESS_THAN -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new LessThanIntConstraint(operands[0]);
            }
            case GREATER_THAN -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new GreaterThanIntConstraint(operands[0]);
            }
            case LESS_THAN_OR_EQUAL -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new LessThanOrEqualIntConstraint(operands[0]);
            }
            case GREATER_THAN_OR_EQUAL -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new GreaterThanOrEqualIntConstraint(operands[0]);
            }
            case NOT_EQUAL -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new NotEqualsIntConstraint(operands[0]);
            }
            case RANGE -> {
                assertTwoOperands(operator.getSymbol(), operands);
                yield new RangeIntConstraint(operands[0], operands[1]);
            }
            case SET -> new SetIntConstraint(operands);
            case LENGTH -> new LengthIntConstraint(operands[0]);
            default -> throw new IllegalArgumentException(
                "The operator '" + operator.getSymbol() + "' is not supported for integer constraints"
            );
        };
    }

    private static void assertSingleOperand(String operator, int[] operands) {
        if (operands.length != 1) {
            throw new IllegalArgumentException("Operator '" + operator + "' expects exactly one operand, but got " + operands.length);
        }
    }

    private static void assertTwoOperands(String operator, int[] operands) {
        if (operands.length != 2) {
            throw new IllegalArgumentException("Operator '" + operator + "' expects exactly two operands, but got " + operands.length);
        }
    }
}
