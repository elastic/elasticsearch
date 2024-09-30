/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.internal.hppc.BitMixer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class CountDistinctTests extends AbstractAggregationTestCase {
    public CountDistinctTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var precisionSuppliers = Stream.of(
            TestCaseSupplier.intCases(0, 100_000, true),
            TestCaseSupplier.longCases(0L, 100_000L, true),
            TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(100_000L), true)
        ).flatMap(List::stream).toList();

        Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.dateCases(1, 1000),
            MultiRowTestCaseSupplier.booleanCases(1, 1000),
            MultiRowTestCaseSupplier.ipCases(1, 1000),
            MultiRowTestCaseSupplier.versionCases(1, 1000),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.KEYWORD),
            MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.TEXT)
        ).flatMap(List::stream).forEach(fieldCaseSupplier -> {
            // With precision
            for (var precisionCaseSupplier : precisionSuppliers) {
                suppliers.add(makeSupplier(fieldCaseSupplier, precisionCaseSupplier));
            }

            // Without precision
            suppliers.add(makeSupplier(fieldCaseSupplier));
        });

        // No rows
        for (var dataType : List.of(
            DataType.INTEGER,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.DATETIME,
            DataType.BOOLEAN,
            DataType.IP,
            DataType.VERSION,
            DataType.KEYWORD,
            DataType.TEXT
        )) {
            var emptyFieldSupplier = new TestCaseSupplier.TypedDataSupplier("No rows (" + dataType + ")", List::of, dataType, false, true);

            // With precision
            for (var precisionCaseSupplier : precisionSuppliers) {
                suppliers.add(makeSupplier(emptyFieldSupplier, precisionCaseSupplier));
            }

            // Without precision
            suppliers.add(makeSupplier(emptyFieldSupplier));
        }

        // "No rows" expects 0 here instead of null
        // return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new CountDistinct(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier precisionSupplier
    ) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type(), precisionSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var precisionTypedData = precisionSupplier.get().forceLiteral();
            var values = fieldTypedData.multiRowData();
            var precision = ((Number) precisionTypedData.data()).intValue();

            long result;

            if (fieldTypedData.type() == DataType.BOOLEAN) {
                result = values.stream().distinct().count();
            } else {
                result = calculateExpectedResult(values, precision);
            }

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, precisionTypedData),
                "CountDistinct[field=Attribute[channel=0],precision=Attribute[channel=1]]",
                DataType.LONG,
                equalTo(result)
            );
        });
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name() + ", no precision", List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var values = fieldTypedData.multiRowData();

            long result;

            if (fieldTypedData.type() == DataType.BOOLEAN) {
                result = values.stream().distinct().count();
            } else {
                result = calculateExpectedResult(values, 3000);
            }

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                "CountDistinct[field=Attribute[channel=0]]",
                DataType.LONG,
                equalTo(result)
            );
        });
    }

    private static long calculateExpectedResult(List<Object> values, int precision) {
        // Can't use driverContext().bigArrays() from a static context
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        try (var hll = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.precisionFromThreshold(precision), bigArrays, 1)) {
            var hash = new MurmurHash3.Hash128();
            for (var value : values) {
                if (value instanceof Integer casted) {
                    hll.collect(0, BitMixer.mix64(casted));
                } else if (value instanceof Long casted) {
                    hll.collect(0, BitMixer.mix64(casted));
                } else if (value instanceof Double casted) {
                    hll.collect(0, BitMixer.mix64(Double.doubleToLongBits(casted)));
                } else if (value instanceof BytesRef casted) {
                    MurmurHash3.hash128(casted.bytes, casted.offset, casted.length, 0, hash);
                    hll.collect(0, BitMixer.mix64(hash.h1));
                } else {
                    throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
                }
            }

            return hll.cardinality(0);
        }
    }
}
