/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;

import java.util.Map;

/**
 * @param rows The number of rows used for query approximation.
 *             If {@code null}, the default will be used.
 *
 * @param confidenceLevel The confidence level of the confidence intervals returned by query approximation.
 *                       If {@code null}, the default will be used.
 */
public record ApproximationSettings(Integer rows, Double confidenceLevel) {
    public static final ApproximationSettings DEFAULT = new ApproximationSettings(null, null);

    public static ApproximationSettings parse(Expression expression) {
        if (expression instanceof Literal literal && literal.value() instanceof Boolean enabled) {
            return enabled ? ApproximationSettings.DEFAULT : null;
        } else if (expression instanceof MapExpression mapExpression) {
            Map<String, Object> map;
            try {
                map = mapExpression.toFoldedMap(FoldContext.small());
            } catch (IllegalStateException ex) {
                throw new IllegalArgumentException("Approximation configuration must be a constant value [" + expression + "]");
            }

            Integer numberOfRows = null;
            Double confidenceLevel = null;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                switch (entry.getKey()) {
                    case "rows":
                        if (entry.getValue() instanceof Integer == false) {
                            throw new IllegalArgumentException("Approximation configuration [rows] must be an integer value");
                        }
                        numberOfRows = (Integer) entry.getValue();
                        if (numberOfRows < 10000) {
                            throw new IllegalArgumentException("Approximation configuration [rows] must be at least 10000");
                        }
                        break;
                    case "confidence_level":
                        if (entry.getValue() instanceof Double == false) {
                            throw new IllegalArgumentException("Approximation configuration [confidence_level] must be a double value");
                        }
                        confidenceLevel = (Double) entry.getValue();
                        if (confidenceLevel < 0.5 || confidenceLevel > 0.95) {
                            throw new IllegalArgumentException(
                                "Approximation configuration [confidence_level] must be between 0.5 and 0.95"
                            );
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Approximation configuration contains unknown key [" + entry.getKey() + "]");
                }
            }
            return new ApproximationSettings(numberOfRows, confidenceLevel);
        } else {
            throw new IllegalArgumentException("Invalid approximation configuration [" + expression + "]");
        }
    }
}
