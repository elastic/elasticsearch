/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

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

public class LastErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(LastTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Last(source, args.get(0), args.get(1));
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // Filter-out the NULL type from the second param (the sort field): it always passes type resolution
        // and the supplier handles it
        return super.testCandidates(cases, valid).filter(sig -> sig.get(1) != DataType.NULL);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(
            typeErrorMessage(
                true,
                validPerPosition,
                signature,
                (v, p) -> p == 0
                    ? "boolean, date, ip, string or numeric except unsigned_long or counter types"
                    : "int or long or date_nanos or datetime"
            )
        );
    }
}
