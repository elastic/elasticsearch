/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DateFormatErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(DateFormatTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateFormat(source, args.get(0), args.size() == 2 ? args.get(1) : null, EsqlTestUtils.TEST_CFG);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        // Single argument version
        String source = sourceForSignature(signature);
        String name = signature.get(0).typeName();
        if (signature.size() == 1) {
            return equalTo("first argument of [" + source + "] must be [datetime or date_nanos], found value [] type [" + name + "]");
        }
        // Two argument version
        // Handle the weird case where we're calling the two argument version with the date first instead of the format.
        if (signature.get(0).isDate()) {
            return equalTo("first argument of [" + source + "] must be [string], found value [] type [" + name + "]");
        }
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (v, p) -> switch (p) {
            case 0 -> "string";
            case 1 -> "datetime or date_nanos";
            default -> "";
        }));
    }
}
