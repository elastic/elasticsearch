/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class HistogramFractionErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(HistogramFractionTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HistogramFraction(source, args.get(0), args.get(1), args.size() == 2 ? null : args.get(2));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.size() == 3
            && signature.get(2) == DataType.NULL
            && validPerPosition.get(0).contains(signature.get(0))
            && validPerPosition.get(1).contains(signature.get(1))) {
            return equalTo("third argument of [" + sourceForSignature(signature) + "] cannot be null, received []");
        }
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (valid, position) -> switch (position) {
            case 0 -> "exponential_histogram or tdigest";
            case 1 -> "double_range";
            case 2 -> "integer";
            default -> throw new AssertionError("unexpected parameter position [" + position + "]");
        }));
    }
}
