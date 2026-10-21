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
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.nullValue;

public class ToDoubleRangeTests extends AbstractScalarFunctionTestCase {
    public ToDoubleRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        // DOUBLE_RANGE passthrough - uses shared doubleRangeCases() so future edge cases are covered
        TestCaseSupplier.forUnaryDoubleRange(suppliers, read, DataType.DOUBLE_RANGE, v -> v, List.of());

        // String types (KEYWORD, TEXT) to DOUBLE_RANGE - parses "start..end" format
        for (DataType stringType : DataType.stringTypes()) {
            suppliers.add(new TestCaseSupplier(stringType.typeName() + " double range string", List.of(stringType), () -> {
                double from = randomDoubleBetween(-1_000_000, 1_000_000, true);
                double to = randomDoubleBetween(from + 1, from + 1_000_000, true);

                String rangeString = from + ".." + to;
                var expectedRange = new DoubleRangeBlockBuilder.DoubleRange(from, to);

                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), stringType, "field")),
                    "ToDoubleRangeFromStringEvaluator[in=" + read + "]",
                    DataType.DOUBLE_RANGE,
                    equalTo(expectedRange)
                );
            }));
        }

        suppliers.addAll(casesForKeyword("1.5..2.5", 1.5, 2.5));
        suppliers.addAll(casesForKeyword("-2.5..-1.5", -2.5, -1.5));
        // Integer-style bounds parse as doubles
        suppliers.addAll(casesForKeyword("1..2", 1.0, 2.0));
        // Scientific notation
        suppliers.addAll(casesForKeyword("1.5e2..2.5e2", 150.0, 250.0));
        // Open bounds are represented as infinities
        suppliers.addAll(casesForKeyword("-Infinity..0.0", Double.NEGATIVE_INFINITY, 0.0));
        suppliers.addAll(casesForKeyword("0.0..Infinity", 0.0, Double.POSITIVE_INFINITY));
        suppliers.addAll(casesForKeyword("-Infinity..Infinity", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));

        // Edge cases: from > to, from == to, NaN bounds, unparseable bounds, missing/extra separators
        suppliers.add(
            caseForKeywordInvalid(
                "from bigger than to",
                "2.5..1.5",
                "java.lang.IllegalArgumentException: double range 'from' [2.5] must be less than 'to' [1.5]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "from same as to",
                "1.5..1.5",
                "java.lang.IllegalArgumentException: double range 'from' [1.5] must be less than 'to' [1.5]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "NaN from",
                "NaN..1.5",
                "java.lang.IllegalArgumentException: double range 'from' [NaN] must be less than 'to' [1.5]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "NaN to",
                "1.5..NaN",
                "java.lang.IllegalArgumentException: double range 'from' [1.5] must be less than 'to' [NaN]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "from unparseable (invalid from)",
                "not-a-double..1.5",
                "java.lang.NumberFormatException: For input string: \"not-a-double\""
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "to unparseable (invalid to)",
                "1.5..not-a-double",
                "java.lang.NumberFormatException: For input string: \"not-a-double\""
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "no range separator",
                "not-a-range",
                "java.lang.IllegalArgumentException: expected double range in the form 'from..to', got [not-a-range]"
            )
        );
        suppliers.add(
            caseForKeywordInvalid(
                "too many separators",
                "1.5..2.5..3.5",
                "java.lang.IllegalArgumentException: expected double range in the form 'from..to', got [1.5..2.5..3.5]"
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    /**
     * Helper to build KEYWORD test cases for TO_DOUBLE_RANGE(string).
     */
    private static List<TestCaseSupplier> casesForKeyword(String rangeString, double expectedFrom, double expectedTo) {
        var expectedRange = new DoubleRangeBlockBuilder.DoubleRange(expectedFrom, expectedTo);
        final String read = "Attribute[channel=0]";
        return List.of(
            new TestCaseSupplier(
                "keyword: " + rangeString,
                List.of(DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.KEYWORD, "field")),
                    "ToDoubleRangeFromStringEvaluator[in=" + read + "]",
                    DataType.DOUBLE_RANGE,
                    equalTo(expectedRange)
                )
            )
        );
    }

    /**
     * Helper for invalid KEYWORD input: parsing throws, evaluator returns null and registers a warning.
     * @param exceptionWarningFragment substring that must appear in the exception warning
     *                                  (e.g. exception message or "IllegalArgumentException")
     */
    private static TestCaseSupplier caseForKeywordInvalid(String name, String rangeString, String exceptionWarningFragment) {
        final String read = "Attribute[channel=0]";
        return new TestCaseSupplier(
            "keyword invalid: " + name,
            List.of(DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef(rangeString), DataType.KEYWORD, "field")),
                "ToDoubleRangeFromStringEvaluator[in=" + read + "]",
                DataType.DOUBLE_RANGE,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: " + exceptionWarningFragment)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDoubleRange(source, args.get(0));
    }

    private static org.hamcrest.Matcher<Object> equalTo(DoubleRangeBlockBuilder.DoubleRange expected) {
        return org.hamcrest.Matchers.equalTo(expected);
    }
}
