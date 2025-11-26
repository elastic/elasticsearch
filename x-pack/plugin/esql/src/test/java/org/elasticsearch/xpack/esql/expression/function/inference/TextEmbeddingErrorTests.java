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

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests error conditions and type validation for TEXT_EMBEDDING function.
 */
public class TextEmbeddingErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TextEmbeddingTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TextEmbedding(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (v, p) -> "string"));
    }

    protected static String typeErrorMessage(
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
                break;
            }
        }

        return ErrorsForCasesWithoutExamplesTestCase.typeErrorMessage(
            includeOrdinal,
            validPerPosition,
            signature,
            positionalErrorMessageSupplier
        );
    }
}
