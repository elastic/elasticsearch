/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomRealisticUnicodeOfLengthBetween;

/**
 * Helper for building {@link TestCaseSupplier}s for unary functions.
 * Create one via {@link TestCaseSupplier#unary()}.
 * <p>
 *     Instances are immutable. Methods returns a new instance. Use it
 *     via chaining, finishing by calling {@link #build}:
 * </p>
 * <pre>{@code
 * List<TestCaseSupplier> suppliers = new ArrayList<>();
 * unary()
 *     .expectedOutputType(DataType.DOUBLE)
 *     .ints()
 *     .expectedFromInt(whatever)
 *     .evaluatorToString("TheEvaluator[val=%0]")
 *     .build(suppliers);
 * }</pre>
 * <p>
 *     All the methods that make inputs <strong>add</strong> to the list of
 *     test cases. So you can call them many times before calling {@link #build}.
 * </p>
 * <pre>{@code
 * unary()
 *     .expectedOutputType(DataType.INTEGER)
 *     .evaluatorToString("LengthEvaluator[val=%0]")
 *     .expectedFromString(s -> UnicodeUtil.codePointCount(new BytesRef(s)))
 *     .strings("empty string", () -> "").expectedFromString(s -> 0);
 *     .strings("single ascii character", () -> "a").expectedFromString(s -> 1)
 *     .build(cases);
 * }</pre>
 * <h2>Share behavior with a static method</h2>
 * <pre>{@code
 * // Make a
 * intHelper().ints(1, Integer.MAX_VALUE).expectedFromInt(whatever).build(suppliers);
 * intHelper().ints(Integer.MIN_VALUE, 0).expectNullAndWarnings(
 *     o -> List.of("Line 1:1: java.lang.ArithmeticException: Log of non-positive number")
 * )
 *
 * static UnaryTestCaseHelper intHelper() {
 *     return unary().expectedOutputType(DataType.DOUBLE)
 *         .evaluatorToString("TheIntEvaluator[val=%0]");
 * }
 * }</pre>
 * <h2>Share behavior by forking the helper</h2>
 * <pre>{@code
 * UnaryTestCaseHelper valid = unary().expectedOutputType(DataType.DOUBLE);
 * valid.ints(1, Integer.MAX_VALUE)
 *      .expectedFromInt(whatever)
 *      .evaluatorToString("TheIntEvaluator[val=%0]")
 *      .build(suppliers);
 * valid.longs(1, Long.MAX_VALUE)
 *      .expectedFromLong(whatever)
 *      .evaluatorToString("TheLongEvaluator[val=%0]")
 *      .build(suppliers);
 * }</pre>
 * <p>
 *     You can also fork that into handling cases that produce warnings as well:
 * </p>
 * <pre>{@code
 * UnaryTestCaseHelper invalid = valid.expectNullAndWarnings(
 *      o -> List.of("Line 1:1: java.lang.ArithmeticException: Log of non-positive number")
 * );
 * invalid.ints(Integer.MIN_VALUE, 0)
 *      .evaluatorToString("TheIntEvaluator[val=%0]")
 *      .build(suppliers);
 * invalid.longs(Long.MIN_VALUE, 0)
 *      .evaluatorToString("TheLongEvaluator[val=%0]")
 *      .build(suppliers);
 * }</pre>
 */
public class UnaryTestCaseHelper extends AbstractTestCaseHelper<UnaryTestCaseHelper> {
    UnaryTestCaseHelper() {
        super(1);
    }

    private UnaryTestCaseHelper(
        List<List<TestCaseSupplier.TypedDataSupplier>> values,
        Function<List<TestCaseSupplier.TypedDataSupplier>, String> name,
        String expectedEvaluatorToString,
        DataType expectedOutputType,
        Function<List<Object>, Object> expected,
        Function<List<Object>, List<String>> expectedWarnings,
        Supplier<Configuration> configuration
    ) {
        super(1, values, name, expectedEvaluatorToString, expectedOutputType, expected, expectedWarnings, configuration);
    }

    /**
     * Add a list of test cases.
     */
    public UnaryTestCaseHelper testCases(List<TestCaseSupplier.TypedDataSupplier> valueSuppliers) {
        return addTestCases(valueSuppliers.stream().map(List::of).toList());
    }

    /**
     * Add a single test case.
     */
    public UnaryTestCaseHelper testCases(TestCaseSupplier.TypedDataSupplier valueSupplier) {
        return addTestCases(List.of(List.of(valueSupplier)));
    }

    /**
     * Add some random string test cases.
     */
    public UnaryTestCaseHelper strings() {
        return strings("empty", () -> "") //
            .strings("short alpha", () -> randomAlphaOfLengthBetween(1, 30))
            .strings("long alpha", () -> randomAlphaOfLengthBetween(300, 3000))
            .strings("short unicode", () -> randomRealisticUnicodeOfLengthBetween(1, 30))
            .strings("long unicode", () -> randomRealisticUnicodeOfLengthBetween(300, 3000));
    }

    /**
     * Add a specific string test case.
     */
    public UnaryTestCaseHelper strings(String name, Supplier<String> supplier) {
        return testCases(
            DataType.stringTypes()
                .stream()
                .map(
                    type -> new TestCaseSupplier.TypedDataSupplier(
                        "<" + name + "::" + type.typeName() + ">",
                        () -> new BytesRef(supplier.get()),
                        type
                    )
                )
                .toList()
        );
    }

    /**
     * Add all possible random int test cases.
     */
    public UnaryTestCaseHelper ints() {
        return ints(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Add some random int test cases with values between {@code min} and {@code max}.
     */
    public UnaryTestCaseHelper ints(int min, int max) {
        return ints(min, max, true);
    }

    /**
     * Add some random int test cases with values between {@code min} and {@code max},
     * optionally skipping {@code 0}.
     */
    public UnaryTestCaseHelper ints(int min, int max, boolean includeZero) {
        return testCases(TestCaseSupplier.intCases(min, max, includeZero));
    }

    /**
     * Add all possible random long test cases.
     */
    public UnaryTestCaseHelper longs() {
        return longs(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Add some random long test cases with values between {@code min} and {@code max}.
     */
    public UnaryTestCaseHelper longs(long min, long max) {
        return longs(min, max, true);
    }

    /**
     * Add some random long test cases with values between {@code min} and {@code max},
     * optionally skipping {@code 0}.
     */
    public UnaryTestCaseHelper longs(long min, long max, boolean includeZero) {
        return testCases(TestCaseSupplier.longCases(min, max, includeZero));
    }

    /**
     * Add some random unsigned long test cases with values between {@code min} and {@code max}.
     */
    public UnaryTestCaseHelper unsignedLongs(BigInteger min, BigInteger max) {
        return unsignedLongs(min, max, true);
    }

    /**
     * Add some random unsigned long test cases with values between {@code min} and {@code max},
     * optionally skipping {@code 0}.
     */
    public UnaryTestCaseHelper unsignedLongs(BigInteger min, BigInteger max, boolean includeZero) {
        // TODO once these are no callers to the static method, move the code here?
        return testCases(TestCaseSupplier.ulongCases(min, max, includeZero));
    }

    /**
     * Add some random double test cases across the full range of double values.
     */
    public UnaryTestCaseHelper doubles() {
        return doubles(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    /**
     * Add some random double test cases with values between {@code min} and {@code max}.
     */
    public UnaryTestCaseHelper doubles(double min, double max) {
        return doubles(min, max, true);
    }

    /**
     * Add some random long test cases with values between {@code min} and {@code max},
     *      * optionally skipping {@code 0}.
     */
    public UnaryTestCaseHelper doubles(double min, double max, boolean includeZero) {
        // TODO once these are no callers to the static method, move the code here?
        return testCases(TestCaseSupplier.doubleCases(min, max, includeZero));
    }

    /**
     * Add all possible random date test cases.
     */
    public UnaryTestCaseHelper dates() {
        return dates(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Add some random date test cases with values between {@code min} and {@code max}
     */
    public UnaryTestCaseHelper dates(long min, long max) {
        return testCases(TestCaseSupplier.dateCases(min, max));
    }

    /**
     * Add all possible random date nanos test cases.
     */
    public UnaryTestCaseHelper dateNanos() {
        return dateNanos(Instant.EPOCH, DateUtils.MAX_NANOSECOND_INSTANT);
    }

    /**
     * Add some random date nanos test cases with values between {@code min} and {@code max}
     */
    public UnaryTestCaseHelper dateNanos(Instant min, Instant max) {
        return testCases(TestCaseSupplier.dateNanosCases(min, max));
    }

    /**
     * Add a random {@code tsid} test case.
     */
    public UnaryTestCaseHelper tsid() {
        return testCases(
            new TestCaseSupplier.TypedDataSupplier(
                "<tsid>",
                () -> EsqlTestUtils.randomLiteral(DataType.TSID_DATA_TYPE).value(),
                DataType.TSID_DATA_TYPE
            )
        );
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expected(Function<Object, Object> expected) {
        return expectedFromArgs(l -> expected.apply(l.getFirst()));
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expectedFromInt(Function<Integer, Object> expected) {
        return expected(o -> expected.apply(((Number) o).intValue()));
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expectedFromLong(Function<Long, Object> expected) {
        return expected(o -> expected.apply(((Number) o).longValue()));
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expectedFromString(Function<String, Object> expected) {
        return expected(o -> expected.apply(((BytesRef) o).utf8ToString()));
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expectedBytesRef(Function<BytesRef, Object> expected) {
        return expectedFromArgs(l -> expected.apply((BytesRef) l.getFirst()));
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expectedFromDouble(Function<Double, Object> expected) {
        return expected(o -> expected.apply(((Number) o).doubleValue()));
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expectedFromBigInteger(Function<BigInteger, Object> expected) {
        return expected(o -> expected.apply((BigInteger) o));
    }

    /**
     * Set a function to build the expected value. If this function returns a
     * {@link Matcher} then we'll {@link Matcher#matches use} it.
     * Otherwise, we'll wrap the result in {@link Matchers#equalTo}.
     */
    public UnaryTestCaseHelper expectedFromInstant(Function<Instant, Object> expected) {
        return expected(o -> expected.apply((Instant) o));
    }

    /**
     * Set a function to build any expected warnings. If this isn't called this will default
     * to expecting no warnings.
     */
    protected UnaryTestCaseHelper expectWarnings(Function<Object, List<String>> expectedWarnings) {
        return expectWarningsFromArgs(l -> expectedWarnings.apply(l.getFirst()));
    }

    /**
     * Set a function to build any expected warnings <strong>and</strong> expect the actual
     * result to be {@code null}. Most functions will return {@code null} when they emit a
     * warning.
     */
    public UnaryTestCaseHelper expectNullAndWarnings(Function<Object, List<String>> expectedWarnings) {
        return expectWarnings(expectedWarnings).expectedFromArgs(o -> null);
    }

    /**
     * Set a function to build any expected warnings <strong>and</strong> expect the actual
     * result to be {@code null}. Most functions will return {@code null} when they emit a
     * warning.
     */
    public UnaryTestCaseHelper expectNullAndWarningsFromString(Function<String, List<String>> expectedWarnings) {
        return expectNullAndWarnings(o -> expectedWarnings.apply(((BytesRef) o).utf8ToString()));
    }

    /**
     * Set a function to build any expected warnings <strong>and</strong> expect the actual
     * result to be {@code null}. Most functions will return {@code null} when they emit a
     * warning.
     */
    public UnaryTestCaseHelper expectNullAndWarningsFromDouble(Function<Double, List<String>> expectedWarnings) {
        return expectNullAndWarnings(o -> expectedWarnings.apply((Double) o));
    }

    /**
     * Set a function to build any expected warnings <strong>and</strong> expect the actual
     * result to be {@code null}. Most functions will return {@code null} when they emit a
     * warning.
     */
    public UnaryTestCaseHelper expectNullAndWarningsFromBigInteger(Function<BigInteger, List<String>> expectedWarnings) {
        return expectNullAndWarnings(o -> expectedWarnings.apply((BigInteger) o));
    }

    /**
     * Build many tests for a function that accepts numeric arguments and casts it's
     * parameters to {@code double} before running. This will test all numeric types
     * that have a value in the range {@code min..=max}.
     * @param min the minimum value to test
     * @param max the maximum value to test
     * @param includeZero should this generate tests for 0?
     * @param suppliers where to write the test cases
     */
    public void castingToDouble(double min, double max, boolean includeZero, List<TestCaseSupplier> suppliers) {
        if (evaluatorToString() == null) {
            throw new IllegalStateException("evaluatorToString must be provided");
        }
        if (values().isEmpty() == false) {
            throw new IllegalStateException("values must *not* be provided");
        }
        if (min > max) {
            throw new IllegalArgumentException("invalid range: " + min + " > " + max);
        }
        var h = expectedOutputType(DataType.DOUBLE);
        h.ints((int) min, (int) max, includeZero).innerCastingToDouble("CastIntToDoubleEvaluator[v=%0]", suppliers);
        h.longs((long) min, (long) max, includeZero).innerCastingToDouble("CastLongToDoubleEvaluator[v=%0]", suppliers);
        h.unsignedLongs(BigInteger.valueOf((long) Math.ceil(min)), BigInteger.valueOf((long) Math.floor(max)), includeZero)
            .innerCastingToDouble("CastUnsignedLongToDoubleEvaluator[v=%0]", suppliers);
        h.doubles(min, max, includeZero).build(suppliers);
    }

    private void innerCastingToDouble(String castEvaluator, List<TestCaseSupplier> suppliers) {
        if (values().isEmpty()) {
            return;
        }
        evaluatorToString(evaluatorToString().replace("%0", castEvaluator)).build(suppliers);
    }

    @Override
    protected UnaryTestCaseHelper create(
        List<List<TestCaseSupplier.TypedDataSupplier>> values,
        Function<List<TestCaseSupplier.TypedDataSupplier>, String> name,
        String expectedEvaluatorToString,
        DataType expectedOutputType,
        Function<List<Object>, Object> expected,
        Function<List<Object>, List<String>> expectedWarnings,
        Supplier<Configuration> configuration
    ) {
        return new UnaryTestCaseHelper(
            values,
            name,
            expectedEvaluatorToString,
            expectedOutputType,
            expected,
            expectedWarnings,
            configuration
        );
    }
}
