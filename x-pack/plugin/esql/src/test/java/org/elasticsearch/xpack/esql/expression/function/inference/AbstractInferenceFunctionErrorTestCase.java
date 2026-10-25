/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for the error-message and type-validation tests of {@link InferenceFunction}s that take
 * {@code (input, inference_id [, options])}. Subclasses supply the valid signatures through {@code cases()} and the
 * function constructor through {@link #buildFunction}. The expected error messages don't vary between subclasses,
 * so this base class generates them once.
 */
public abstract class AbstractInferenceFunctionErrorTestCase extends ErrorsForCasesWithoutExamplesTestCase {

    /**
     * Build the inference function under test. {@code options} is {@code null} for the two-argument form.
     */
    protected abstract Expression buildFunction(Source source, Expression input, Expression inferenceId, Expression options);

    @Override
    protected final Expression build(Source source, List<Expression> args) {
        return buildFunction(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(inferenceTypeErrorMessage(true, validPerPosition, signature, (v, p) -> "string"));
    }

    /**
     * Inference functions report two kinds of error the generic machinery doesn't produce. First,
     * {@code isNotNull} rejects a {@code null} argument before any type check runs. Second, the trailing options
     * argument must be a map expression, not just any accepted-looking value.
     */
    protected static String inferenceTypeErrorMessage(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> signature,
        AbstractFunctionTestCase.PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        for (int i = 0; i < signature.size(); i++) {
            if (signature.get(i) == DataType.NULL) {
                String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(i).name().toLowerCase(Locale.ROOT) + " " : "";
                return ordinal + "argument of [" + sourceForSignature(signature) + "] cannot be null, received []";
            }

            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                // Map expressions have different error messages
                if (i == 2) {
                    return format(null, "third argument of [{}] must be a map expression, received []", sourceForSignature(signature));
                }
                break;
            }
        }

        return typeErrorMessage(includeOrdinal, validPerPosition, signature, positionalErrorMessageSupplier);
    }
}
