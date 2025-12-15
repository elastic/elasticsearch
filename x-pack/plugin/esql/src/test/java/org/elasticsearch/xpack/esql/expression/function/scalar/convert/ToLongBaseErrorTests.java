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

import java.util.List;
import java.util.Set;

// import org.elasticsearch.test.junit.annotations.TestLogging;
// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ToLongBaseErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(ToLongBaseTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToLongBase(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> unused, List<DataType> signature) {
        switch (signature.get(0)) {
            case DataType.KEYWORD:
            case DataType.TEXT:
            case DataType.NULL:
                break;
            default:
                return typeErrorMessage(signature, 0, "string");
        }
        switch (signature.get(1)) {
            case DataType.INTEGER:
            case DataType.NULL:
                break;
            default:
                return typeErrorMessage(signature, 1, "integer");
        }
        throw new IllegalStateException("signature is not an error: " + signature);
    }
}
