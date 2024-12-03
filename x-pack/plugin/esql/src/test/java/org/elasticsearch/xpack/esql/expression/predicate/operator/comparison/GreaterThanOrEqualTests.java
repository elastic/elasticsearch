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

public class GreaterThanOrEqualTests extends AbstractScalarFunctionTestCase {
    public GreaterThanOrEqualTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
                        (l, r) -> l.intValue() >= r.intValue(),
                        "GreaterThanOrEqualIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() >= r.longValue(),
                        "GreaterThanOrEqualLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> l.doubleValue() >= r.doubleValue(),
                        "GreaterThanOrEqualDoublesEvaluator"
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
                "GreaterThanOrEqualLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> ((BigInteger) l).compareTo((BigInteger) r) >= 0,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "GreaterThanOrEqualKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> ((BytesRef) l).compareTo((BytesRef) r) >= 0,
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.ipCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "GreaterThanOrEqualKeywordsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> ((BytesRef) l).compareTo((BytesRef) r) >= 0,
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.versionCases(""),
                List.of(),
                false
            )
        );
        // Datetime
        suppliers.addAll(TestCaseSupplier.forBinaryNotCasting("GreaterThanOrEqualLongsEvaluator", "lhs", "rhs", (lhs, rhs) -> {
            if (lhs instanceof Instant l && rhs instanceof Instant r) {
                return l.isAfter(r) || l.equals(r);
            }
            throw new UnsupportedOperationException("Got some weird types");
        }, DataType.BOOLEAN, TestCaseSupplier.dateCases(), TestCaseSupplier.dateCases(), List.of(), false));

        suppliers.addAll(TestCaseSupplier.forBinaryNotCasting("GreaterThanOrEqualLongsEvaluator", "lhs", "rhs", (lhs, rhs) -> {
            if (lhs instanceof Instant l && rhs instanceof Instant r) {
                return l.isAfter(r) || l.equals(r);
            }
            throw new UnsupportedOperationException("Got some weird types");
        }, DataType.BOOLEAN, TestCaseSupplier.dateNanosCases(), TestCaseSupplier.dateNanosCases(), List.of(), false));

        suppliers.addAll(
            TestCaseSupplier.stringCases(
                (l, r) -> ((BytesRef) l).compareTo((BytesRef) r) >= 0,
                (lhsType, rhsType) -> "GreaterThanOrEqualKeywordsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                List.of(),
                DataType.BOOLEAN
            )
        );

        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(
                anyNullIsNull(true, suppliers),
                (o, v, t) -> AbstractScalarFunctionTestCase.errorMessageStringForBinaryOperators(
                    o,
                    v,
                    t,
                    (l, p) -> "date_nanos, datetime, double, integer, ip, keyword, long, semantic_text, text, unsigned_long or version"
                )
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new GreaterThanOrEqual(source, args.get(0), args.get(1));
    }
}
