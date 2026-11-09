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

public class TimeSeriesTransformErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        // Only the one-label signatures: error generation permutes every argument position, which longer signatures explode.
        return paramsToSuppliers(TimeSeriesTransformTests.parameters()).stream().filter(c -> c.types().size() == 2).toList();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesTransform(source, args.getFirst(), args.subList(1, args.size()));
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // A null label container is the empty one, so only the label can be wrong there; the label rules have their own test.
        return super.testCandidates(cases, valid).filter(types -> types.getFirst() != DataType.NULL);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return typeErrorMessage(signature, 0, "_timeseries");
    }
}
