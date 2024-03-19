/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class ToDatetimeTests extends AbstractFunctionTestCase {
    public ToDatetimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryDatetime(suppliers, read, DataTypes.DATETIME, Instant::toEpochMilli, emptyList());

        TestCaseSupplier.forUnaryInt(
            suppliers,
            "ToLongFromIntEvaluator[field=" + read + "]",
            DataTypes.DATETIME,
            i -> ((Integer) i).longValue(),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            emptyList()
        );
        TestCaseSupplier.forUnaryLong(suppliers, read, DataTypes.DATETIME, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, emptyList());
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[field=" + read + "]",
            DataTypes.DATETIME,
            BigInteger::longValueExact,
            BigInteger.ZERO,
            BigInteger.valueOf(Long.MAX_VALUE),
            emptyList()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[field=" + read + "]",
            DataTypes.DATETIME,
            bi -> null,
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TWO),
            UNSIGNED_LONG_MAX,
            bi -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + bi + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToLongFromDoubleEvaluator[field=" + read + "]",
            DataTypes.DATETIME,
            d -> null,
            Double.NEGATIVE_INFINITY,
            -9.223372036854777E18, // a "convenient" value smaller than `(double) Long.MIN_VALUE` (== ...776E18)
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToLongFromDoubleEvaluator[field=" + read + "]",
            DataTypes.DATETIME,
            d -> null,
            9.223372036854777E18, // a "convenient" value larger than `(double) Long.MAX_VALUE` (== ...776E18)
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            "ToDatetimeFromStringEvaluator[field=" + read + "]",
            DataTypes.DATETIME,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: "
                    + (bytesRef.utf8ToString().isEmpty()
                        ? "cannot parse empty datetime"
                        : ("failed to parse date field [" + bytesRef.utf8ToString() + "] with format [yyyy-MM-dd'T'HH:mm:ss.SSS'Z']"))
            )
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToDatetimeFromStringEvaluator[field=" + read + "]",
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "<date string>",
                    // millis past "0001-01-01T00:00:00.000Z" to match the default formatter
                    () -> new BytesRef(randomDateString(-62135596800000L, Long.MAX_VALUE)),
                    DataTypes.KEYWORD
                )
            ),
            DataTypes.DATETIME,
            bytesRef -> DateParse.DEFAULT_FORMATTER.parseMillis(((BytesRef) bytesRef).utf8ToString()),
            emptyList()
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToDatetimeFromStringEvaluator[field=" + read + "]",
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "<date string before 0001-01-01T00:00:00.000Z>",
                    // millis before "0001-01-01T00:00:00.000Z"
                    () -> new BytesRef(randomDateString(Long.MIN_VALUE, -62135596800001L)),
                    DataTypes.KEYWORD
                )
            ),
            DataTypes.DATETIME,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: failed to parse date field ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "] with format [yyyy-MM-dd'T'HH:mm:ss.SSS'Z']"
            )
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    private static String randomDateString(long from, long to) {
        String result = Instant.ofEpochMilli(randomLongBetween(from, to)).toString();
        if (result.matches(".*:..Z")) {
            // it's a zero millisecond date string, Instant.toString() will strip the milliseconds (and the parsing will fail)
            return result.replace("Z", ".000Z");
        }
        return result;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDatetime(source, args.get(0));
    }
}
