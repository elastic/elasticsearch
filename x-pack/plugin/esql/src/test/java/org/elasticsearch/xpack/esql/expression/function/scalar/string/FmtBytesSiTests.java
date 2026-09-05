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
import org.elasticsearch.common.Strings;
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

public class FmtBytesSiTests extends AbstractScalarFunctionTestCase {

    public FmtBytesSiTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
        suppliers.add(unitSupplier(500, "B", "500B"));
        suppliers.add(intUnitSupplier(2000, "KB", "2KB"));
        suppliers.add(unitSupplier(2000, "KB", "2KB"));
        suppliers.add(unitSupplier(3_000_000L, "MB", "3MB"));
        suppliers.add(unitSupplier(4_000_000_000L, "GB", "4GB"));
        suppliers.add(unitSupplier(5_000_000_000_000L, "TB", "5TB"));
        suppliers.add(unitSupplier(6_000_000_000_000_000L, "PB", "6PB"));
        suppliers.add(unitSupplier(-1, "KB", "-1B"));
        suppliers.add(negativeSupplier(-2));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public void testUnitIsCaseInsensitive() {
        assertThat(FmtBytesSi.formatWithUnit(2000, "kb"), equalTo("2KB"));
    }

    public void testUnknownUnitThrows() {
        var e = expectThrows(IllegalArgumentException.class, () -> FmtBytesSi.formatWithUnit(1500, "eb"));
        assertThat(e.getMessage(), equalTo("Unsupported unit [eb], expected one of [B, KB, MB, GB, TB, PB]"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FmtBytesSi(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    /**
     * Independent SI (base-1000) reference implementation used to verify the evaluator output.
     */
    private static String formatBytesSi(long bytes) {
        double value = bytes;
        String suffix = "B";
        if (bytes >= 1_000_000_000_000_000L) {
            value = bytes / 1.0e15;
            suffix = "PB";
        } else if (bytes >= 1_000_000_000_000L) {
            value = bytes / 1.0e12;
            suffix = "TB";
        } else if (bytes >= 1_000_000_000L) {
            value = bytes / 1.0e9;
            suffix = "GB";
        } else if (bytes >= 1_000_000L) {
            value = bytes / 1.0e6;
            suffix = "MB";
        } else if (bytes >= 1_000L) {
            value = bytes / 1.0e3;
            suffix = "KB";
        }
        return Strings.format1Decimals(value, suffix);
    }

    private static TestCaseSupplier makeIntSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            int value = ((Number) fieldTypedData.data()).intValue();
            String evaluatorToString = "FmtBytesSiFromIntEvaluator[bytes=Attribute[channel=0]]";
            BytesRef expected = new BytesRef(formatBytesSi(value));
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier makeLongSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            long value = ((Number) fieldTypedData.data()).longValue();
            String evaluatorToString = "FmtBytesSiFromLongEvaluator[bytes=Attribute[channel=0]]";
            BytesRef expected = new BytesRef(formatBytesSi(value));
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier unitSupplier(long bytes, String unit, String expected) {
        return new TestCaseSupplier("bytes=" + bytes + ", unit=" + unit, List.of(DataType.LONG, DataType.KEYWORD), () -> {
            var bytesTypedData = new TestCaseSupplier.TypedData(bytes, DataType.LONG, "bytes");
            var unitTypedData = new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.KEYWORD, "unit").forceLiteral();
            return new TestCaseSupplier.TestCase(
                List.of(bytesTypedData, unitTypedData),
                "FmtBytesSiFromLongWithUnitEvaluator[bytes=Attribute[channel=0], unit=LiteralsEvaluator[lit=" + unit + "]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        });
    }

    private static TestCaseSupplier intUnitSupplier(int bytes, String unit, String expected) {
        return new TestCaseSupplier("bytes=" + bytes + ", unit=" + unit, List.of(DataType.INTEGER, DataType.KEYWORD), () -> {
            var bytesTypedData = new TestCaseSupplier.TypedData(bytes, DataType.INTEGER, "bytes");
            var unitTypedData = new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.KEYWORD, "unit").forceLiteral();
            return new TestCaseSupplier.TestCase(
                List.of(bytesTypedData, unitTypedData),
                "FmtBytesSiFromIntWithUnitEvaluator[bytes=Attribute[channel=0], unit=LiteralsEvaluator[lit=" + unit + "]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            );
        });
    }

    private static TestCaseSupplier negativeSupplier(int bytes) {
        return new TestCaseSupplier("negative bytes=" + bytes, List.of(DataType.INTEGER), () -> {
            var bytesTypedData = new TestCaseSupplier.TypedData(bytes, DataType.INTEGER, "bytes");
            TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                List.of(bytesTypedData),
                "FmtBytesSiFromIntEvaluator[bytes=Attribute[channel=0]]",
                DataType.KEYWORD,
                nullValue()
            );
            return testCase.withWarning(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
            ).withWarning("Line 1:1: java.lang.IllegalArgumentException: Values less than [-1] bytes are not supported: [" + bytes + "]");
        });
    }
}
