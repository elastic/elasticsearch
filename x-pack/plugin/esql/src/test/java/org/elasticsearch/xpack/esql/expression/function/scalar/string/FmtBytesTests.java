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
import org.elasticsearch.common.unit.ByteSizeValue;
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

public class FmtBytesTests extends AbstractScalarFunctionTestCase {

    public FmtBytesTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
        suppliers.add(unitSupplier(512, "b", "512b"));
        suppliers.add(intUnitSupplier(2048, "kb", "2kb"));
        suppliers.add(unitSupplier(2048, "kb", "2kb"));
        suppliers.add(unitSupplier(3 * 1024L * 1024, "mb", "3mb"));
        suppliers.add(unitSupplier(4 * 1024L * 1024 * 1024, "gb", "4gb"));
        suppliers.add(unitSupplier(5 * 1024L * 1024 * 1024 * 1024L, "tb", "5tb"));
        suppliers.add(unitSupplier(6 * 1024L * 1024 * 1024 * 1024L * 1024L, "pb", "6pb"));
        suppliers.add(unitSupplier(-1, "kb", "-1b"));
        suppliers.add(negativeSupplier(-2));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public void testUnitIsCaseInsensitive() {
        assertThat(FmtBytes.formatWithUnit(2048, "KB"), equalTo("2kb"));
    }

    public void testUnknownUnitThrows() {
        var e = expectThrows(IllegalArgumentException.class, () -> FmtBytes.formatWithUnit(1536, "eb"));
        assertThat(e.getMessage(), equalTo("Unsupported unit [eb], expected one of [b, kb, mb, gb, tb, pb]"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FmtBytes(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    private static TestCaseSupplier makeIntSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            int value = ((Number) fieldTypedData.data()).intValue();
            String evaluatorToString = "FmtBytesFromIntEvaluator[bytes=Attribute[channel=0]]";
            BytesRef expected = new BytesRef(ByteSizeValue.ofBytes(value).toString());
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier makeLongSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            long value = ((Number) fieldTypedData.data()).longValue();
            String evaluatorToString = "FmtBytesFromLongEvaluator[bytes=Attribute[channel=0]]";
            BytesRef expected = new BytesRef(ByteSizeValue.ofBytes(value).toString());
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier unitSupplier(long bytes, String unit, String expected) {
        return new TestCaseSupplier("bytes=" + bytes + ", unit=" + unit, List.of(DataType.LONG, DataType.KEYWORD), () -> {
            var bytesTypedData = new TestCaseSupplier.TypedData(bytes, DataType.LONG, "bytes");
            var unitTypedData = new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.KEYWORD, "unit").forceLiteral();
            return new TestCaseSupplier.TestCase(
                List.of(bytesTypedData, unitTypedData),
                "FmtBytesFromLongWithUnitEvaluator[bytes=Attribute[channel=0], unit=LiteralsEvaluator[lit=" + unit + "]]",
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
                "FmtBytesFromIntWithUnitEvaluator[bytes=Attribute[channel=0], unit=LiteralsEvaluator[lit=" + unit + "]]",
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
                "FmtBytesFromIntEvaluator[bytes=Attribute[channel=0]]",
                DataType.KEYWORD,
                nullValue()
            );
            return testCase.withWarning(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
            ).withWarning("Line 1:1: java.lang.IllegalArgumentException: Values less than -1 bytes are not supported: " + bytes + "b");
        });
    }
}
