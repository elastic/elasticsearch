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

import static org.hamcrest.Matchers.containsString;

public class TimeSeriesMetadataPathErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TimeSeriesMetadataPathTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesMetadataPath(source, args.get(0), args.get(1));
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // A null metadata is the empty one, so only the key can be wrong there; the key rules have their own test.
        return super.testCandidates(cases, valid).filter(types -> types.getFirst() != DataType.NULL);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.getFirst() == DataType.KEYWORD) {
            // The metadata is a keyword _timeseries, so only the key can be wrong.
            return signature.get(1) == DataType.NULL
                ? containsString("must be non-empty, single-valued strings")
                : typeErrorMessage(signature, 1, "string");
        }
        return typeErrorMessage(signature, 0, "_timeseries");
    }
}
