/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class TimeSeriesMetadataSetErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TimeSeriesMetadataSetTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataSet(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // A null label container is the empty one and a null value removes the label; the label rules have their own test.
        return super.testCandidates(cases, valid).filter(
            types -> types.get(0) != DataType.NULL && types.get(1) == DataType.KEYWORD && types.get(2) != DataType.NULL
        );
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return signature.getFirst() == DataType.TSMD_DATA_TYPE
            ? typeErrorMessage(signature, 2, "string")
            : typeErrorMessage(signature, 0, "_timeseries");
    }
}
