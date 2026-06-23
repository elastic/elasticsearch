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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MvUnionErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MvUnionTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvUnion(source, args.get(0), args.get(1));
    }

    /**
     * MvUnion treats null as an empty set, so {@code MV_UNION(null, T)} and {@code MV_UNION(T, null)}
     * resolve successfully for any representable type T. Skip those signatures — the corresponding
     * non-null versions (e.g. {@code [histogram, histogram]}) still cover the unsupported-type errors.
     */
    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        return super.testCandidates(cases, valid).filter(signature -> {
            if (signature.get(0) == DataType.NULL) {
                return valid.stream().noneMatch(v -> v.get(1).equals(signature.get(1)));
            }
            if (signature.get(1) == DataType.NULL) {
                return valid.stream().noneMatch(v -> v.get(0).equals(signature.get(0)));
            }
            return true;
        });
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        var unsupportedTypes = List.of(
            DataType.AGGREGATE_METRIC_DOUBLE,
            DataType.DENSE_VECTOR,
            DataType.EXPONENTIAL_HISTOGRAM,
            DataType.HISTOGRAM,
            DataType.TDIGEST,
            DataType.DATE_RANGE
        );
        if (signature.getFirst() == DataType.NULL && unsupportedTypes.contains(signature.get(1))) {
            // resolveType() checks the non-null (second) arg; the error names that arg's type, not "null"
            return containsString(
                "argument of ["
                    + sourceForSignature(signature)
                    + "] must be [any type except counter types, dense_vector, "
                    + "aggregate_metric_double, tdigest, histogram, exponential_histogram, or date_range"
                    + "], found value [] type ["
                    + signature.get(1).typeName()
                    + "]"
            );
        } else if (unsupportedTypes.contains(signature.getFirst())) {
            return containsString(
                typeErrorMessage(
                    false,
                    validPerPosition,
                    signature,
                    (v, p) -> "any type except counter types, dense_vector, "
                        + "aggregate_metric_double, tdigest, histogram, exponential_histogram, or date_range"
                )
            );
        } else {
            return equalTo(
                "second argument of ["
                    + sourceForSignature(signature)
                    + "] must be ["
                    + signature.get(0).noText().typeName()
                    + "], found value [] type ["
                    + signature.get(1).typeName()
                    + "]"
            );
        }
    }
}
