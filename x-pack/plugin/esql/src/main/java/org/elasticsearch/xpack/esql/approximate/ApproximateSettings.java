/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;

import java.util.Map;

public record ApproximateSettings(
    Integer rows,
    Double confidenceLevel
) {
    public static final ApproximateSettings DEFAULT = new ApproximateSettings(null, null);

    public static ApproximateSettings parse(Expression expression) {
        if (expression instanceof Literal literal && literal.value() instanceof Boolean enabled) {
            return enabled ? ApproximateSettings.DEFAULT : null;
        } else if (expression instanceof MapExpression mapExpression) {
            Map<String, Object> map;
            try {
                map = mapExpression.toFoldedMap(FoldContext.small());
            } catch (IllegalStateException ex) {
                throw new IllegalArgumentException("Approximate configuration must be a constant value [" + expression + "]");
            }

            Integer numberOfRows = null;
            Double confidenceLevel = null;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                switch (entry.getKey()) {
                    case "rows":
                        if (entry.getValue() instanceof Integer == false) {
                            throw new IllegalArgumentException("Approximate configuration [rows] must be an integer value");
                        }
                        numberOfRows = (Integer) entry.getValue();
                        break;
                    case "confidence_level":
                        if (entry.getValue() instanceof Double == false) {
                            throw new IllegalArgumentException("Approximate configuration [confidence_level] must be a double value");
                        }
                        confidenceLevel = (Double) entry.getValue();
                        break;
                    default:
                        throw new IllegalArgumentException("Approximate configuration contains unknown key [" + entry.getKey() + "]");
                }
            }
            return new ApproximateSettings(numberOfRows, confidenceLevel);
        } else {
            throw new IllegalArgumentException("Invalid approximate configuration [" + expression + "]");
        }
    }
}
