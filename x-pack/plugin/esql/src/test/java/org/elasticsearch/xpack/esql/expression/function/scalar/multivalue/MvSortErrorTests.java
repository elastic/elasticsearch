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

import static org.hamcrest.Matchers.equalTo;

public class MvSortErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MvSortTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSort(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (validPerPosition.get(0).contains(signature.get(0)) && signature.size() > 1 && signature.get(1) == DataType.NULL) {
            return equalTo("second argument of [" + sourceForSignature(signature) + "] cannot be null, received []");
        }
        return equalTo(typeErrorMessage(true, validPerPosition, signature, (v, p) -> switch (p) {
            case 0 -> "not unsigned long or geo";
            case 1 -> "string";
            default -> throw new UnsupportedOperationException();
        }));
    }
}
