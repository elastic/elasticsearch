/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class MatchOperatorErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MatchOperatorTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MatchOperator(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(
            errorMessageStringForMatch(validPerPosition, signature, (l, p) -> p == 0 ? FIELD_TYPE_ERROR_STRING : QUERY_TYPE_ERROR_STRING)
        );
    }

    private static String errorMessageStringForMatch(
        List<Set<DataType>> validPerPosition,
        List<DataType> signature,
        AbstractFunctionTestCase.PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        boolean invalid = false;
        for (int i = 0; i < signature.size() && invalid == false; i++) {
            // Need to check for nulls and bad parameters in order
            if (signature.get(i) == DataType.NULL) {
                return TypeResolutions.ParamOrdinal.fromIndex(i).name().toLowerCase(Locale.ROOT)
                    + " argument of ["
                    + sourceForSignature(signature)
                    + "] cannot be null, received []";
            }
            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                break;
            }
        }

        try {
            return typeErrorMessage(true, validPerPosition, signature, positionalErrorMessageSupplier);
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is for nulls or from the combination
            return EsqlBinaryComparison.formatIncompatibleTypesMessage(signature.get(0), signature.get(1), sourceForSignature(signature));
        }
    }

    private static final String FIELD_TYPE_ERROR_STRING =
        "keyword, text, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version";

    private static final String QUERY_TYPE_ERROR_STRING =
        "keyword, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version";
}
