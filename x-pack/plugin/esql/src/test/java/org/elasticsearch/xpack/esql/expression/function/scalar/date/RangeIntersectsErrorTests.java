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
 * Error tests for RANGE_INTERSECTS(left, right). Both arguments must be date or date_range.
 */
public class RangeIntersectsErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        assumeTrue("DATE_RANGE type is only supported in snapshot builds", DataType.DATE_RANGE.supportedVersion().supportedLocally());
        return paramsToSuppliers(RangeIntersectsTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RangeIntersects(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (v, i) -> "date or date_range"));
    }
}
