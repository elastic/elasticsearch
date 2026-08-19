/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class BucketErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(BucketTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return switch (args.size()) {
            case 2 -> new Bucket(source, args.get(0), args.get(1), null, null, null, EsqlTestUtils.TEST_CFG);
            case 4 -> new Bucket(source, args.get(0), args.get(1), args.get(2), args.get(3), null, EsqlTestUtils.TEST_CFG);
            default -> throw new IllegalArgumentException("unexpected BUCKET arity " + args.size());
        };
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        DataType field = signature.get(0);
        DataType buckets = signature.get(1);
        boolean fourArgs = signature.size() == 4;

        if (field == DataType.DATETIME || field == DataType.DATE_NANOS) {
            if (isSupportedBucketsWholeNumber(buckets) == false
                && DataType.isTemporalAmount(buckets) == false
                && buckets != DataType.NULL) {
                return typeErrorMessage(signature, 1, "integral, date_period or time_duration");
            }
            if (isSupportedBucketsWholeNumber(buckets) || fourArgs) {
                if (fourArgs == false) {
                    return arityError(field, buckets, "four");
                }
                if (isStringOrDate(signature.get(2)) == false) {
                    return typeErrorMessage(signature, 2, "datetime, date_nanos or string");
                }
                if (isStringOrDate(signature.get(3)) == false) {
                    return typeErrorMessage(signature, 3, "datetime, date_nanos or string");
                }
            }
        } else if (field.isNumeric()) {
            if (isSupportedBucketsNumeric(buckets) == false && buckets != DataType.NULL) {
                return typeErrorMessage(signature, 1, "integer, long or double");
            }
            if (buckets.isRationalNumber()) {
                if (fourArgs) {
                    return arityError(field, buckets, "two");
                }
            } else if (fourArgs) {
                if (isNumericOrNull(signature.get(2)) == false) {
                    return typeErrorMessage(signature, 2, "numeric");
                }
                if (isNumericOrNull(signature.get(3)) == false) {
                    return typeErrorMessage(signature, 3, "numeric");
                }
            }
        } else if (field == DataType.NULL) {
            if (isSupportedBucketsNumeric(buckets) == false && DataType.isTemporalAmount(buckets) == false && buckets != DataType.NULL) {
                return typeErrorMessage(signature, 1, "numeric, date_period or time_duration");
            }
            if (fourArgs) {
                if (buckets.isRationalNumber()) {
                    return arityError(field, buckets, "two");
                }
                if (isStringOrDateOrNumeric(signature.get(2)) == false) {
                    return typeErrorMessage(signature, 2, "datetime, date_nanos, string or numeric");
                }
                if (isStringOrDateOrNumeric(signature.get(3)) == false) {
                    return typeErrorMessage(signature, 3, "datetime, date_nanos, string or numeric");
                }
            }
        } else {
            return typeErrorMessage(signature, 0, "datetime or numeric");
        }
        throw new IllegalStateException("can't find bad arg for " + signature);
    }

    private static Matcher<String> arityError(DataType field, DataType buckets, String expectedCount) {
        return equalTo(
            "function expects exactly "
                + expectedCount
                + " arguments when the first one is of type ["
                + field
                + "] and the second of type ["
                + buckets
                + "]"
        );
    }

    private static boolean isSupportedBucketsWholeNumber(DataType dt) {
        return dt.isWholeNumber() && dt != DataType.UNSIGNED_LONG;
    }

    private static boolean isSupportedBucketsNumeric(DataType dt) {
        return dt.isNumeric() && dt != DataType.UNSIGNED_LONG;
    }

    private static boolean isNumericOrNull(DataType dt) {
        return dt == DataType.NULL || dt.isNumeric();
    }

    private static boolean isStringOrDate(DataType dt) {
        return dt == DataType.NULL || DataType.isString(dt) || DataType.isMillisOrNanos(dt);
    }

    private static boolean isStringOrDateOrNumeric(DataType dt) {
        return isStringOrDate(dt) || dt.isNumeric();
    }
}
