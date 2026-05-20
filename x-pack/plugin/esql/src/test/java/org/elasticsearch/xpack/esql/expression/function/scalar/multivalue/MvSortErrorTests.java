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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class MvSortErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MvSortTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSort(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    /**
     * MvSort's order parameter accepts any string (KEYWORD or TEXT), but test cases only use KEYWORD.
     * Filter out TEXT-at-position-1 signatures when the KEYWORD variant is valid — they resolve fine too.
     */
    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        return super.testCandidates(cases, valid).filter(signature -> {
            if (signature.size() > 1) {
                // isString accepts TEXT as well as KEYWORD; filter TEXT when the KEYWORD variant is valid
                if (signature.get(1) == DataType.TEXT) {
                    List<DataType> withKeyword = new ArrayList<>(signature);
                    withKeyword.set(1, DataType.KEYWORD);
                    if (valid.contains(withKeyword)) {
                        return false;
                    }
                }
                // isType special-cases DataType.NULL (always passes), so (T, null) always resolves;
                // covered by (T, keyword) variants
                if (signature.get(1) == DataType.NULL) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        // MvSort checks position 0 (representable) then position 1 (string), in that order.
        // Not all valid types appear in test cases, so validPerPosition[0] is incomplete — we check
        // representability directly instead of relying on validPerPosition.
        if (isUnrepresentable(signature.get(0))) {
            return equalTo(
                "first argument of ["
                    + sourceForSignature(signature)
                    + "] must be [any type except counter types, dense_vector, "
                    + "aggregate_metric_double, tdigest, histogram, exponential_histogram, or date_range"
                    + "], found value [] type ["
                    + signature.get(0).typeName()
                    + "]"
            );
        }
        // Position 0 is representable (or NULL, which isType always accepts); error is at position 1.
        return equalTo(
            "second argument of ["
                + sourceForSignature(signature)
                + "] must be [string], found value [] type ["
                + signature.get(1).typeName()
                + "]"
        );
    }

    private static boolean isUnrepresentable(DataType type) {
        return type.isCounter()
            || type == DataType.DENSE_VECTOR
            || type == DataType.AGGREGATE_METRIC_DOUBLE
            || type == DataType.TDIGEST
            || type == DataType.HISTOGRAM
            || type == DataType.EXPONENTIAL_HISTOGRAM
            || type == DataType.DATE_RANGE;
    }
}
