/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

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

public class MvLikeErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    private static final String SUPPORTED_TYPES = "string";

    @Override
    protected List<TestCaseSupplier> cases() {
        // Deriving the case universe from the unit test's own parameters is what keeps the error surface from drifting
        // away from the accepted-signature surface.
        return paramsToSuppliers(MvLikeTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvLike(source, args.get(0), args.get(1));
    }

    /**
     * Mirrors {@link MvLike}'s resolveType: the pattern must be a string type. A null-typed literal pattern is an author
     * error, but null is type-compatible so it passes resolveType and is rejected later in postOptimizationVerification
     * (see the testCandidates filter below) — not here. The field is more lenient: a null-typed field resolves and folds
     * to {@code false} at runtime, per the two-valued contract, so it carries no error message.
     */
    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // A null-typed pattern (position 1) resolves at analysis — null is type-compatible — and is rejected later in
        // postOptimizationVerification, not resolveType. So don't fuzz it as an "expected unresolved" invalid signature.
        return super.testCandidates(cases, valid).filter(sig -> sig.get(1) != DataType.NULL);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        DataType field = signature.get(0);
        DataType pattern = signature.get(1);
        if (field != DataType.NULL && DataType.isString(field) == false) {
            return typeErrorMessage(signature, 0, SUPPORTED_TYPES);
        }
        if (pattern != DataType.NULL && DataType.isString(pattern) == false) {
            return typeErrorMessage(signature, 1, SUPPORTED_TYPES);
        }
        return equalTo("");
    }
}
