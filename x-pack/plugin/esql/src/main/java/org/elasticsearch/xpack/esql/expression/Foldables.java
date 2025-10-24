/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.common.Failure.fail;

public abstract class Foldables {
    /**
     * A utility class to validate the type resolution of expressions before and after logical planning.
     * If null is passed for Failures to the constructor, it means we are only type resolution.
     * This is usually called when doing pre-logical planning validation.
     * If a {@link Failures} instance is passed, it means we are doing post-logical planning validation as well.
     * This is usually called after folding is done, during
     * {@link org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware} verification
     */
    public static class TypeResolutionValidator {

        Expression.TypeResolution typeResolution = Expression.TypeResolution.TYPE_RESOLVED;
        @Nullable
        private final Failures postValidationFailures; // null means we are doing pre-folding validation only
        private final Expression field;

        public static TypeResolutionValidator forPreOptimizationValidation(Expression field) {
            return new TypeResolutionValidator(field, null);
        }

        public static TypeResolutionValidator forPostOptimizationValidation(Expression field, Failures failures) {
            return new TypeResolutionValidator(field, failures);
        }

        private TypeResolutionValidator(Expression field, Failures failures) {
            this.field = field;
            this.postValidationFailures = failures;
        }

        public void invalidIfPostValidation(Failure failure) {
            if (postValidationFailures != null) {
                postValidationFailures.add(failure);
            }
        }

        public void invalid(Expression.TypeResolution message) {
            typeResolution = message;
            if (postValidationFailures != null) {
                postValidationFailures.add(fail(field, message.message()));
            }
        }

        public Expression.TypeResolution getResolvedType() {
            return typeResolution;
        }
    }

    public static Object valueOf(FoldContext ctx, Expression e) {
        if (e.foldable()) {
            return e.fold(ctx);
        }
        throw new QlIllegalArgumentException("Cannot determine value for {}", e);
    }

    public static String stringLiteralValueOf(Expression expression, String message) {
        if (expression instanceof Literal literal && literal.value() instanceof BytesRef bytesRef) {
            return bytesRef.utf8ToString();
        }
        throw new QlIllegalArgumentException(message);
    }

    public static Object literalValueOf(Expression e) {
        if (e instanceof Literal literal) {
            return literal.value();
        }
        throw new QlIllegalArgumentException("Expected literal, but got {}", e);
    }

    public static Object extractLiteralOrReturnSelf(Expression e) {
        if (e instanceof Literal literal) {
            return literal.value();
        }
        return e;
    }

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
     * During postOptimizationVerification folding is already done, so we also verify that it is definitively a literal
     */
    public static Expression.TypeResolution resolveTypeLimit(Expression limitField, String sourceText, TypeResolutionValidator validator) {
        if (limitField == null) {
            validator.invalid(
                new Expression.TypeResolution(format(null, "Limit must be a constant integer in [{}], found [{}]", sourceText, limitField))
            );
        } else if (limitField instanceof Literal literal) {
            if (literal.value() == null) {
                validator.invalid(
                    new Expression.TypeResolution(
                        format(null, "Limit must be a constant integer in [{}], found [{}]", sourceText, limitField)
                    )
                );
            } else {
                int value = (Integer) literal.value();
                if (value <= 0) {
                    validator.invalid(
                        new Expression.TypeResolution(format(null, "Limit must be greater than 0 in [{}], found [{}]", sourceText, value))
                    );
                }
            }
        } else {
            // it is expected that the expression is a literal after folding
            // we fail if it is not a literal
            validator.invalidIfPostValidation(
                fail(limitField, "Limit must be a constant integer in [{}], found [{}]", sourceText, limitField)
            );
        }
        return validator.getResolvedType();
    }

    /**
     * We check that the query is not null and that if it is a literal, it is a string
     * During postOptimizationVerification folding is already done, so we also verify that it is definitively a literal
     */
    public static Expression.TypeResolution resolveTypeQuery(Expression queryField, String sourceText, TypeResolutionValidator validator) {
        if (queryField == null) {
            validator.invalid(
                new Expression.TypeResolution(format(null, "Query must be a valid string in [{}], found [{}]", sourceText, queryField))
            );
        } else if (queryField instanceof Literal literal) {
            if (literal.value() == null) {
                validator.invalid(
                    new Expression.TypeResolution(format(null, "Query value cannot be null in [{}], but got [{}]", sourceText, queryField))
                );
            }
        } else {
            // it is expected that the expression is a literal after folding
            // we fail if it is not a literal
            validator.invalidIfPostValidation(fail(queryField, "Query must be a valid string in [{}], found [{}]", sourceText, queryField));
        }
        return validator.getResolvedType();
    }

    public static Object queryAsObject(Expression queryField, String sourceText) {
        if (queryField instanceof Literal literal) {
            return literal.value();
        }
        throw new EsqlIllegalArgumentException(
            format(null, "Query value must be a constant string in [{}], found [{}]", sourceText, queryField)
        );
    }

    public static String queryAsString(Expression queryField, String sourceText) {
        if (queryField instanceof Literal literal) {
            return BytesRefs.toString(literal.value());
        }
        throw new EsqlIllegalArgumentException(
            format(null, "Query value must be a constant string in [{}], found [{}]", sourceText, queryField)
        );
    }

    public static int intValueOf(Expression field, String sourceText, String fieldName) {
        if (field instanceof Literal literal && literal.value() instanceof Number n) {
            return n.intValue();
        }
        throw new EsqlIllegalArgumentException(
            Strings.format(null, "[{}] value must be a constant number in [{}], found [{}]", fieldName, sourceText, field)
        );
    }

    public static double doubleValueOf(Expression field, String sourceText, String fieldName) {
        if (field instanceof Literal literal && literal.value() instanceof Number n) {
            return n.doubleValue();
        }
        throw new EsqlIllegalArgumentException(
            Strings.format(null, "[{}] value must be a constant number in [{}], found [{}]", fieldName, sourceText, field)
        );
    }
}
