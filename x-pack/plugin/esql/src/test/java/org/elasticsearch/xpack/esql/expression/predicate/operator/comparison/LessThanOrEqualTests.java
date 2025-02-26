/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class LessThanOrEqualTests extends AbstractScalarFunctionTestCase {
    public LessThanOrEqualTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryComparisonWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.intValue() <= r.intValue(),
                        "LessThanOrEqualIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() <= r.longValue(),
                        "LessThanOrEqualLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> l.doubleValue() <= r.doubleValue(),
                        "LessThanOrEqualDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> List.of(),
                false
            )
        );

        // Unsigned Long cases
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "LessThanOrEqualLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> ((BigInteger) l).compareTo((BigInteger) r) <= 0,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "LessThanOrEqualKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> ((BytesRef) l).compareTo((BytesRef) r) <= 0,
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.ipCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "LessThanOrEqualKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> ((BytesRef) l).compareTo((BytesRef) r) <= 0,
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.versionCases(""),
                List.of(),
                false
            )
        );
        // Datetime
        suppliers.addAll(TestCaseSupplier.forBinaryNotCasting("LessThanOrEqualLongsEvaluator", "lhs", "rhs", (lhs, rhs) -> {
            if (lhs instanceof Instant l && rhs instanceof Instant r) {
                return l.isBefore(r) || l.equals(r);
            }
            throw new UnsupportedOperationException("Got some weird types");
        }, DataType.BOOLEAN, TestCaseSupplier.dateCases(), TestCaseSupplier.dateCases(), List.of(), false));

        suppliers.addAll(TestCaseSupplier.forBinaryNotCasting("LessThanOrEqualLongsEvaluator", "lhs", "rhs", (lhs, rhs) -> {
            if (lhs instanceof Instant l && rhs instanceof Instant r) {
                return l.isBefore(r) || l.equals(r);
            }
            throw new UnsupportedOperationException("Got some weird types");
        }, DataType.BOOLEAN, TestCaseSupplier.dateNanosCases(), TestCaseSupplier.dateNanosCases(), List.of(), false));

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "LessThanOrEqualNanosMillisEvaluator",
                "lhs",
                "rhs",
                (l, r) -> (((Instant) l).isBefore((Instant) r) || l.equals(r)),
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "LessThanOrEqualMillisNanosEvaluator",
                "lhs",
                "rhs",
                (l, r) -> (((Instant) l).isBefore((Instant) r) || l.equals(r)),
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateNanosCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.stringCases(
                (l, r) -> ((BytesRef) l).compareTo((BytesRef) r) <= 0,
                (lhsType, rhsType) -> "LessThanOrEqualKeywordsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                List.of(),
                DataType.BOOLEAN
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new LessThanOrEqual(source, args.get(0), args.get(1), null);
    }
}
