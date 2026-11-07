/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.union;

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
 * The types this refuses are exactly the ones no union leg can hold, because {@code resolveFieldType} asks
 * {@code UnionEsField.isLegType} rather than carrying its own list. So the cases in {@link TypeOfTests} have to name
 * every leg type: one missing there shows up here as a signature the harness expected to be refused and was not.
 */
public class TypeOfErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TypeOfTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TypeOf(source, args.get(0));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(typeErrorMessage(false, validPerPosition, signature, (v, p) -> "any type a column of more than one type can hold"));
    }
}
