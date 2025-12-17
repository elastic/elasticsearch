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

public class MvDedupeErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MvDedupeTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvDedupe(source, args.get(0));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(
            typeErrorMessage(
                false,
                validPerPosition,
                signature,
                (v, p) -> "any type except counter types, dense_vector, " +
                    "aggregate_metric_double, tdigest, histogram, or exponential_histogram"
            )
        );
    }

    @Override
    protected void assertCheckedSignatures(Set<List<DataType>> invalidSignatureSamples) {
        /*
         * In general MvDedupe should support all signatures. While building a
         * new type you may we to temporarily relax this.
         */
        assertThat(
            invalidSignatureSamples,
            equalTo(
                Set.of(
                    List.of(DataType.DENSE_VECTOR),
                    List.of(DataType.EXPONENTIAL_HISTOGRAM),
                    List.of(DataType.AGGREGATE_METRIC_DOUBLE),
                    List.of(DataType.HISTOGRAM),
                    List.of(DataType.TDIGEST)
                )
            )
        );
    }

}
