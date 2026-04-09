/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class RLikeErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(RLikeTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        /*
         * We can't support certain signatures, and it's safe not to test them because
         * you can't even build them.... The building comes directly from the parser
         * and can only make certain types.
         */
        return super.testCandidates(cases, valid).filter(sig -> sig.get(1) == DataType.KEYWORD)
            .filter(sig -> sig.size() > 2 && sig.get(2) == DataType.BOOLEAN);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return RLikeTests.buildRLike(logger, source, args);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(typeErrorMessage(false, validPerPosition, signature, (v, p) -> "string"));
    }
}
