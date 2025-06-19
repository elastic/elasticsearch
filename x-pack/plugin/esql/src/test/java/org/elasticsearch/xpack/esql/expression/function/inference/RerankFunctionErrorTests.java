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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.hamcrest.Matchers.equalTo;

public class RerankFunctionErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(RerankFunctionTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // Don't test null, as it is not allowed but the expected message is not a type error - so we check it separately in VerifierTests
        return super.testCandidates(cases, valid).filter(sig -> false == sig.contains(DataType.NULL));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RerankFunction(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(errorMessageString(validPerPosition, signature, (v, p) -> "string"));
    }

    private static String errorMessageString(
        List<Set<DataType>> validPerPosition,
        List<DataType> signature,
        AbstractFunctionTestCase.PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        for (int i = 0; i < signature.size(); i++) {
            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                // Map expressions have different error messages
                if (i == signature.size() - 1) {
                    return format(
                        null,
                        "{} argument of [{}] must be a map expression, received []",
                        TypeResolutions.ParamOrdinal.fromIndex(i).name().toLowerCase(Locale.ROOT),
                        sourceForSignature(signature)
                    );
                }
                break;
            }
        }

        return typeErrorMessage(true, validPerPosition, signature, positionalErrorMessageSupplier);
    }
}
