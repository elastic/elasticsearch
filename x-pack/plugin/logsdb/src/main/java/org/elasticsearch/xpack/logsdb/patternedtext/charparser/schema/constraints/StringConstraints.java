/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.OperatorType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.PatternUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class StringConstraints {

    /**
     * Parses a raw constraint string from schema.yaml and returns a {@link StringConstraint}
     * that implements the constraint.
     * Handles:
     * <ul>
     *     <li>Simple constraints: {@code ==value}, {@code !=value}, {@code Jan|Feb|Mar}, {@code {3}}</li>
     *     <li>Logical operators: {@code &&} (AND), {@code ||} (OR)</li>
     * </ul>
     *
     * @param rawConstraint the constraint string as defined in schema.yaml
     * @return a {@link StringConstraint} that evaluates the constraint
     */
    public static StringConstraint parseStringConstraint(String rawConstraint) {
        if (rawConstraint == null || rawConstraint.trim().isEmpty()) {
            return AnyString.INSTANCE;
        }

        String constraint = rawConstraint.trim();

        // Handle logical operators first
        String[] chainedConstraints = PatternUtils.splitChainedAndConstraints(constraint);
        if (chainedConstraints.length == 2) {
            return parseStringConstraint(chainedConstraints[0]).and(parseStringConstraint(chainedConstraints[1]));
        }
        chainedConstraints = PatternUtils.splitChainedOrConstraints(constraint);
        if (chainedConstraints.length == 2) {
            return parseStringConstraint(chainedConstraints[0]).or(parseStringConstraint(chainedConstraints[1]));
        }

        // Handle simple constraints
        PatternUtils.ConstraintDetails details = PatternUtils.parseSimpleConstraint(constraint);
        return createConstraint(details.operator(), details.operands());
    }

    /**
     * Creates a {@link StringConstraint} based on the given operator and operands.
     *
     * @param operator the operator type from OperatorType enum
     * @param operands the string operands for the constraint
     * @return a {@link StringConstraint} that evaluates the constraint
     * @throws IllegalArgumentException if the operator is unsupported or operands are invalid
     */
    public static StringConstraint createConstraint(OperatorType operator, String... operands) {
        return switch (operator) {
            case EQUALITY -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new EqualsStringConstraint(operands[0]);
            }
            case NOT_EQUAL -> {
                assertSingleOperand(operator.getSymbol(), operands);
                yield new NotEqualsStringConstraint(operands[0]);
            }
            case SET -> new StringSetConstraint(new HashSet<>(Arrays.asList(operands)));
            case MAP -> {
                if (operands.length % 2 != 0) {
                    throw new IllegalArgumentException("Map operator requires an even number of operands, got: " + operands.length);
                }
                Map<String, Integer> map = new HashMap<>();
                for (int i = 0; i < operands.length; i += 2) {
                    String key = operands[i];
                    try {
                        int value = Integer.parseInt(operands[i + 1]);
                        map.put(key, value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Map operator requires numeric values, got: " + operands[i + 1]);
                    }
                }
                yield new StringToIntMapConstraint(map);
            }
            case LENGTH -> {
                assertSingleOperand(operator.getSymbol(), operands);
                try {
                    int length = Integer.parseInt(operands[0]);
                    yield new LengthStringConstraint(length);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Length constraint requires a numeric value, got: " + operands[0]);
                }
            }
            default -> throw new IllegalArgumentException(
                "The operator '" + operator.getSymbol() + "' is not supported for string constraints"
            );
        };
    }

    private static void assertSingleOperand(String operator, String[] operands) {
        if (operands.length != 1) {
            throw new IllegalArgumentException("Operator '" + operator + "' expects exactly one operand, but got " + operands.length);
        }
    }
}
