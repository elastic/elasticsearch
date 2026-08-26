/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FmtDurationTests extends AbstractScalarFunctionTestCase {

    public FmtDurationTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (var supplier : TestCaseSupplier.intCases(0, Integer.MAX_VALUE, true)) {
            suppliers.add(makeIntSupplier(supplier));
        }
        for (var supplier : TestCaseSupplier.longCases(0, Long.MAX_VALUE, true)) {
            suppliers.add(makeLongSupplier(supplier));
        }
        suppliers.add(unitSupplier(500L, "nanos", "500nanos"));
        suppliers.add(intUnitSupplier(500, "nanos", "500nanos"));
        suppliers.add(unitSupplier(2_500L, "micros", "2.5micros"));
        suppliers.add(unitSupplier(1_500_000_000L, "ms", "1500ms"));
        suppliers.add(unitSupplier(2_500_000_000L, "s", "2.5s"));
        suppliers.add(unitSupplier(90_000_000_000L, "m", "1.5m"));
        suppliers.add(unitSupplier(7_200_000_000_000L, "h", "2h"));
        suppliers.add(unitSupplier(172_800_000_000_000L, "d", "2d"));
        suppliers.add(negativeSupplier(-2));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public void testUnitIsCaseInsensitive() {
        assertThat(FmtDuration.formatWithUnit(1_500_000_000L, "MS"), equalTo("1500ms"));
    }

    public void testUnknownUnitThrows() {
        var e = expectThrows(IllegalArgumentException.class, () -> FmtDuration.formatWithUnit(1_500_000_000L, "fortnight"));
        assertThat(e.getMessage(), equalTo("Unsupported unit [fortnight], expected one of [nanos, micros, ms, s, m, h, d]"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FmtDuration(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    private static TestCaseSupplier makeIntSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            int value = ((Number) fieldTypedData.data()).intValue();
            String evaluatorToString = "FmtDurationFromIntEvaluator[nanoseconds=Attribute[channel=0]]";
            BytesRef expected = new BytesRef(TimeValue.timeValueNanos(value).toHumanReadableString(1));
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier makeLongSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            long value = ((Number) fieldTypedData.data()).longValue();
            String evaluatorToString = "FmtDurationFromLongEvaluator[nanoseconds=Attribute[channel=0]]";
            BytesRef expected = new BytesRef(TimeValue.timeValueNanos(value).toHumanReadableString(1));
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier unitSupplier(long nanoseconds, String unit, String expected) {
        return new TestCaseSupplier("nanoseconds=" + nanoseconds + ", unit=" + unit, List.of(DataType.LONG, DataType.KEYWORD), () -> {
            var nanosTypedData = new TestCaseSupplier.TypedData(nanoseconds, DataType.LONG, "nanoseconds");
            var unitTypedData = new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.KEYWORD, "unit").forceLiteral();
            return new TestCaseSupplier.TestCase(
                List.of(nanosTypedData, unitTypedData),
                "FmtDurationFromLongWithUnitEvaluator[nanoseconds=Attribute[channel=0], unit=LiteralsEvaluator[lit=" + unit + "]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        });
    }

    private static TestCaseSupplier intUnitSupplier(int nanoseconds, String unit, String expected) {
        return new TestCaseSupplier("nanoseconds=" + nanoseconds + ", unit=" + unit, List.of(DataType.INTEGER, DataType.KEYWORD), () -> {
            var nanosTypedData = new TestCaseSupplier.TypedData(nanoseconds, DataType.INTEGER, "nanoseconds");
            var unitTypedData = new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.KEYWORD, "unit").forceLiteral();
            return new TestCaseSupplier.TestCase(
                List.of(nanosTypedData, unitTypedData),
                "FmtDurationFromIntWithUnitEvaluator[nanoseconds=Attribute[channel=0], unit=LiteralsEvaluator[lit=" + unit + "]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        });
    }

    private static TestCaseSupplier negativeSupplier(int nanoseconds) {
        return new TestCaseSupplier("negative nanoseconds=" + nanoseconds, List.of(DataType.INTEGER), () -> {
            var nanosTypedData = new TestCaseSupplier.TypedData(nanoseconds, DataType.INTEGER, "nanoseconds");
            TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                List.of(nanosTypedData),
                "FmtDurationFromIntEvaluator[nanoseconds=Attribute[channel=0]]",
                DataType.KEYWORD,
                nullValue()
            );
            return testCase.withWarning(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
            ).withWarning("Line 1:1: java.lang.IllegalArgumentException: duration cannot be negative, was given [" + nanoseconds + "]");
        });
    }
}
