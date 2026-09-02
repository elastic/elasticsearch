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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class CountErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(CountTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return args.size() == 1 ? new Count(source, args.get(0)) : new Count(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.size() == 2) {
            if (signature.get(0) != DataType.EXPONENTIAL_HISTOGRAM && signature.get(0) != DataType.TDIGEST) {
                return equalTo(
                    "argument of ["
                        + sourceForSignature(signature)
                        + "] must be [exponential_histogram or tdigest], found value [] type ["
                        + signature.get(0).typeName()
                        + "]"
                );
            }
            return typeErrorMessage(signature, 1, "double_range");
        }
        return equalTo(typeErrorMessage(false, validPerPosition, signature, (v, p) -> "any type except counter types or histogram"));
    }

    @Override
    protected void assertCheckedSignatures(Set<List<DataType>> invalidSignatureSamples) {
        assertThat(invalidSignatureSamples, hasItem(List.of(DataType.HISTOGRAM)));
    }
}
