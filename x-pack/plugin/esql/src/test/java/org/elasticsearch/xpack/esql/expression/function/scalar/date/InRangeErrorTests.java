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
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Error tests for InRange(date, date_range) -> boolean.
 * These validate type resolution without requiring an evaluator implementation.
 */
public class InRangeErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(InRangeTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new InRange(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        // First arg must be datetime or date_nanos, second must be date_range
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (v, i) -> {
            if (i == 0) {
                return "date or date_nanos";
            }
            if (i == 1) {
                return "date_range";
            }
            return "";
        }));
    }
}
