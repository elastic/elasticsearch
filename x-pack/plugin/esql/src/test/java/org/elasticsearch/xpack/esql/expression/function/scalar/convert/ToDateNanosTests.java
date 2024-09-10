/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToDateNanosTests extends AbstractScalarFunctionTestCase {
    public ToDateNanosTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryDateNanos(suppliers, read, DataType.DATE_NANOS, DateUtils::toLong, List.of());
        TestCaseSupplier.forUnaryDatetime(
            suppliers,
            "ToDateNanosFromDatetimeEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            0,
            DateUtils.MAX_NANOSECOND_INSTANT.toEpochMilli(),
            i -> DateUtils.toNanoSeconds(i.toEpochMilli()),
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToDateNanosFromLongEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            l -> l,
            0,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToDateNanosFromLongEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            l -> null,
            Long.MIN_VALUE,
            -1L,
            List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported."
            )
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            BigInteger::longValueExact,
            BigInteger.ZERO,
            BigInteger.valueOf(Long.MAX_VALUE),
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            bi -> null,
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TWO),
            UNSIGNED_LONG_MAX,
            bi -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + bi + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToDateNanosFromDoubleEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            d -> null,
            Double.NEGATIVE_INFINITY,
            -Double.MIN_VALUE,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported."
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToDateNanosFromDoubleEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            d -> null,
            9.223372036854777E18, // a "convenient" value larger than `(double) Long.MAX_VALUE` (== ...776E18)
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            "ToDateNanosFromStringEvaluator[field=" + read + "]",
            DataType.DATE_NANOS,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: "
                    + (bytesRef.utf8ToString().isEmpty()
                        ? "cannot parse empty datetime"
                        : ("failed to parse date field [" + bytesRef.utf8ToString() + "] with format [strict_date_optional_time_nanos]"))
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            suppliers,
            (v, p) -> "date_nanos or datetime or double or long or string or unsigned_long"
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDateNanos(source, args.get(0));
    }
}
