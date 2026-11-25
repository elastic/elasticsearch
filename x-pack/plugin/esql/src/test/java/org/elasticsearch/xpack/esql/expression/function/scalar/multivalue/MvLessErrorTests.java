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

public class MvLessErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    private static final String SUPPORTED_TYPES = MvCompare.SUPPORTED_TYPES;

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MvLessTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvLess(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        return super.testCandidates(cases, valid).filter(sig -> sig.size() == 2 || sig.get(2) == DataType.UNSUPPORTED);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        DataType field = signature.get(0);
        if (field == DataType.NULL) {
            DataType bound = signature.get(1);
            if (bound != DataType.NULL && isSupported(bound) == false) {
                return typeErrorMessage(signature, 1, SUPPORTED_TYPES);
            }
            return equalTo("");
        }
        if (isSupported(field) == false) {
            return typeErrorMessage(signature, 0, SUPPORTED_TYPES);
        }
        DataType bound = signature.get(1);
        if (bound != DataType.NULL && bound.noText() != field.noText()) {
            return typeErrorMessage(signature, 1, field.noText().typeName());
        }
        return equalTo("");
    }

    private static boolean isSupported(DataType dt) {
        return MvCompare.isSupportedRangeType(dt);
    }
}
