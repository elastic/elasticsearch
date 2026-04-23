/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// import org.elasticsearch.test.junit.annotations.TestLogging;
// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ToLongSurrogateErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        return paramsToSuppliers(ToLongSurrogateTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        if (args.size() == 1) {
            return new ToLongSurrogate(source, args.get(0), null);
        } else if (args.size() == 2) {
            return new ToLongSurrogate(source, args.get(0), args.get(1));
        } else {
            throw new IllegalArgumentException("Unexpected number of arguments: " + args.size());
        }
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.size() == 1) {
            return new ToLongErrorTests().expectedTypeErrorMatcher(validPerPosition, signature);
        } else if (signature.size() == 2) {
            return new ToLongBaseErrorTests().expectedTypeErrorMatcher(validPerPosition, signature);
        } else {
            throw new IllegalArgumentException("Unexpected number of arguments: " + signature.size());
        }
    }
}
