/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class FunctionUtils {
    public static Integer limitValue(Expression limitField, String sourceText) {
        if (limitField instanceof Literal literal) {
            Object value = literal.value();
            if (value instanceof Integer intValue) {
                return intValue;
            }
        }
        throw new EsqlIllegalArgumentException(format(null, "Limit value must be an integer in [{}], found [{}]", sourceText, limitField));
    }

    /**
     * We check that the limit is not null and that if it is a literal, it is a positive integer
     * We will do a more thorough check in the postOptimizationVerification once folding is done.
     */
    public static Expression.TypeResolution resolveTypeLimit(Expression limitField, String sourceText) {
        if (limitField == null) {
            return new Expression.TypeResolution(format(null, "Limit must be a constant integer in [{}], found [{}]", sourceText, limitField));
        }
        if (limitField instanceof Literal literal) {
            if (literal.value() == null) {
                return new Expression.TypeResolution(
                    format(null, "Limit must be a constant integer in [{}], found [{}]", sourceText, limitField)
                );
            }
            int value = (Integer) literal.value();
            if (value <= 0) {
                return new Expression.TypeResolution(format(null, "Limit must be greater than 0 in [{}], found [{}]", sourceText, value));
            }
        }
        return Expression.TypeResolution.TYPE_RESOLVED;
    }
    public static void postOptimizationVerificationLimit(Failures failures, Expression limitField, String sourceText) {
        if (limitField == null) {
            failures.add(fail(limitField, "Limit must be a constant integer in [{}], found [{}]", sourceText, limitField));
        }
        if (limitField instanceof Literal literal) {
            int value = (Integer) literal.value();
            if (value <= 0) {
                failures.add(fail(limitField, "Limit must be greater than 0 in [{}], found [{}]", sourceText, value));
            }
        } else {
            // it is expected that the expression is a literal after folding
            // we fail if it is not a literal
            failures.add(fail(limitField, "Limit must be a constant integer in [{}], found [{}]", sourceText, limitField));
        }
    }

}
