/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import static org.hamcrest.Matchers.equalTo;

public class DateParseErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(DateParseTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateParse(source, args.get(0), args.size() > 1 ? args.get(1) : null, args.size() > 2 ? args.get(2) : null);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        // If the signature has 3 parameters and ONLY the third is invalid, we need to check for the MapExpression error
        if (signature.size() == 3 && validPerPosition.size() >= 3) {
            // Check if the first two parameters are valid
            boolean firstParamValid = validPerPosition.get(0).isEmpty() == false
                && (signature.get(0) == null || validPerPosition.get(0).contains(signature.get(0)));
            boolean secondParamValid = validPerPosition.get(1).isEmpty() == false
                && (signature.get(1) == null || validPerPosition.get(1).contains(signature.get(1)));

            // Check if the third parameter is invalid
            boolean thirdParamInvalid = validPerPosition.get(2).isEmpty()
                || (validPerPosition.get(2).contains(signature.get(2)) == false && signature.get(2) != null);

            // Only use the MapExpression error format if the first two are valid but the third is invalid
            if (firstParamValid && secondParamValid && thirdParamInvalid) {
                // The third parameter uses isMapExpression which has a different error format
                String ordinal = ParamOrdinal.THIRD.name().toLowerCase(Locale.ROOT);
                return equalTo(ordinal + " argument of [" + sourceForSignature(signature) + "] must be a map expression, received []");
            }
        }

        // For all other cases (including when multiple params are invalid), use the standard error format
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (v, i) -> "string"));
    }
}
