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

public class ToRangeErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        assumeTrue("DATE_RANGE type is only supported in snapshot builds", DataType.DATE_RANGE.supportedVersion().supportedLocally());
        return paramsToSuppliers(ToRangeTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToRange(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        DataType fromType = signature.get(0);
        if (fromType != DataType.DATETIME && fromType != DataType.DOUBLE && fromType != DataType.NULL) {
            return typeErrorMessage(signature, 0, "date or double");
        }
        String expected = fromType == DataType.NULL ? "date or double" : fromType.esType();
        return typeErrorMessage(signature, 1, expected);
    }
}
