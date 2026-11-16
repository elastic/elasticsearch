/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.validFunctionParameters;
import static org.hamcrest.Matchers.equalTo;

public class ConfidenceIntervalErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(ConfidenceIntervalTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // Five parameters are too many for all permutations, so every valid signature is checked with one argument replaced by each type.
        return valid.stream()
            .flatMap(
                signature -> IntStream.range(0, signature.size())
                    .boxed()
                    .flatMap(position -> validFunctionParameters().map(type -> replace(signature, position, type)))
            )
            .distinct()
            .filter(signature -> valid.contains(signature) == false)
            .filter(signature -> signature.stream().filter(t -> t == DataType.NULL).count() <= 1);
    }

    private static List<DataType> replace(List<DataType> signature, int position, DataType type) {
        List<DataType> replaced = new ArrayList<>(signature);
        replaced.set(position, type);
        return replaced;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ConfidenceInterval(source, args.get(0), args.get(1), args.get(2), args.get(3), args.get(4));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (v, p) -> p == 2 || p == 3 ? "integer" : "double"));
    }
}
