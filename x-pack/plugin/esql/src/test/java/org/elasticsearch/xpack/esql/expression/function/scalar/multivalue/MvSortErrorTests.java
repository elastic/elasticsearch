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

import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLATTENED;
import static org.elasticsearch.xpack.esql.core.type.DataType.HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.TDIGEST;
import static org.hamcrest.Matchers.equalTo;

public class MvSortErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    private static final String FIELD_TYPES = "any type except counter, spatial types, dense_vector, "
        + "aggregate_metric_double, tdigest, histogram, exponential_histogram, date_range, or flattened";

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MvSortTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSort(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    /**
     * Positive tests don't cover every legal shape {@code MV_SORT} accepts (default {@code order},
     * {@code text} {@code order}, {@code null} {@code order}). Those still type-check, so exclude them
     * here unless the field type itself is invalid.
     */
    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        return super.testCandidates(cases, valid).filter(types -> {
            DataType field = types.get(0);
            if (types.size() == 1) {
                return isInvalidFieldType(field);
            }
            DataType order = types.get(1);
            if (order == DataType.NULL || DataType.isString(order)) {
                return isInvalidFieldType(field);
            }
            return true;
        });
    }

    private static boolean isInvalidFieldType(DataType type) {
        if (type == DataType.NULL) {
            return false;
        }
        return DataType.isRepresentable(type) == false
            || DataType.isSpatialOrGrid(type)
            || type == DENSE_VECTOR
            || type == AGGREGATE_METRIC_DOUBLE
            || type == EXPONENTIAL_HISTOGRAM
            || type == HISTOGRAM
            || type == TDIGEST
            || type == DATE_RANGE
            || type == FLATTENED;
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        // Prefer resolveType order over test-derived validPerPosition: some accepted field types
        // (e.g. unsigned_long) are missing from MvSortTests, which would mis-attribute the bad arg.
        String source = sourceForSignature(signature);
        if (isInvalidFieldType(signature.get(0))) {
            return equalTo(
                "first argument of ["
                    + source
                    + "] must be ["
                    + FIELD_TYPES
                    + "], found value [] type ["
                    + signature.get(0).typeName()
                    + "]"
            );
        }
        return equalTo("second argument of [" + source + "] must be [string], found value [] type [" + signature.get(1).typeName() + "]");
    }
}
