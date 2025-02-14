/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class MvMedianAbsoluteDeviationTests extends AbstractMultivalueFunctionTestCase {
    public MvMedianAbsoluteDeviationTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        doubles(cases, "mv_median_absolute_deviation", "MvMedianAbsoluteDeviation", (size, valuesStream) -> {
            var values = valuesStream.sorted().toArray();
            int middle = size / 2;
            if (size % 2 == 1) {
                double median = values[middle];
                return equalTo(Arrays.stream(values).map(d -> Math.abs(d - median)).sorted().skip(middle).findFirst().orElseThrow());
            } else {
                double median = (values[middle - 1] + values[middle]) / 2;
                return equalTo(
                    Arrays.stream(values).map(d -> Math.abs(d - median)).sorted().skip(middle - 1).limit(2).average().orElseThrow()
                );
            }
        });
        ints(cases, "mv_median_absolute_deviation", "MvMedianAbsoluteDeviation", (size, values) -> {
            var mad = calculateMedianAbsoluteDeviation(size, values.mapToObj(BigInteger::valueOf));
            return equalTo(mad.intValue());
        });
        longs(cases, "mv_median_absolute_deviation", "MvMedianAbsoluteDeviation", (size, values) -> {
            var mad = calculateMedianAbsoluteDeviation(size, values.mapToObj(BigInteger::valueOf));
            return equalTo(mad.longValue());
        });
        unsignedLongs(cases, "mv_median_absolute_deviation", "MvMedianAbsoluteDeviation", (size, values) -> {
            var mad = calculateMedianAbsoluteDeviation(size, values);
            return equalTo(mad);
        });

        // Simple cases
        cases.addAll(makeCases(List.of(1, 2, 5), 1, true));
        cases.addAll(makeCases(List.of(1, 2), 0, false));
        cases.addAll(makeCases(List.of(-1, -2), 0, false));
        cases.addAll(makeCases(List.of(0, 2, 5, 6), 2, false));

        // Overflow cases
        cases.addAll(
            overflowCasesFor(
                DataType.INTEGER,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE / 2,
                Integer.MAX_VALUE / 2 + 1
            )
        );
        cases.addAll(
            overflowCasesFor(DataType.LONG, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE / 2L, Long.MAX_VALUE / 2L + 1)
        );
        cases.addAll(
            overflowCasesFor(
                DataType.DOUBLE,
                Double.MAX_VALUE,
                -Double.MAX_VALUE,
                Double.MAX_VALUE,
                Double.MAX_VALUE / 2.,
                Double.MAX_VALUE / 2.
            )
        );
        cases.addAll(
            overflowCasesFor(
                DataType.UNSIGNED_LONG,
                NumericUtils.asLongUnsigned(NumericUtils.UNSIGNED_LONG_MAX),
                NumericUtils.ZERO_AS_UNSIGNED_LONG,
                NumericUtils.UNSIGNED_LONG_MAX.divide(BigInteger.valueOf(2)),
                NumericUtils.UNSIGNED_LONG_MAX.divide(BigInteger.valueOf(2)),
                BigInteger.ZERO
            )
        );

        // Custom double overflow. Can't be checked in the generic overflow cases, as "MAX_DOUBLE + 1000 == MAX_DOUBLE"
        cases.add(
            new TestCaseSupplier(
                "mv_median_absolute_deviation(<min_double, big_number, same_big_number>)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            List.of(-Double.MAX_VALUE, Double.MAX_VALUE / 4, Double.MAX_VALUE / 4),
                            DataType.DOUBLE,
                            "field"
                        )
                    ),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    equalTo(0.)
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, cases);
    }

    /**
     * Makes cases for the given data, for each type
     */
    private static List<TestCaseSupplier> makeCases(List<Number> data, Number expectedResult, boolean withDoubles) {
        var types = new ArrayList<>(List.of(DataType.INTEGER, DataType.LONG));
        if (withDoubles) {
            types.add(DataType.DOUBLE);
        }
        if (data.stream().noneMatch(d -> d.doubleValue() < 0)) {
            types.add(DataType.UNSIGNED_LONG);
        }
        return types.stream().map(type -> {
            var convertedData = data.stream().map(d -> {
                var convertedValue = DataTypeConverter.convert(d, type);
                if (convertedValue instanceof BigInteger bi) {
                    return NumericUtils.asLongUnsigned(bi);
                }
                return convertedValue;
            }).toList();

            return new TestCaseSupplier(
                "<" + convertedData + "> (" + type + ")",
                List.of(type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(convertedData, type, "field")),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    type,
                    equalTo(DataTypeConverter.convert(expectedResult, type))
                )
            );
        }).toList();
    }

    private static List<TestCaseSupplier> overflowCasesFor(
        DataType type,
        Number max,
        Number min,
        Number maxMinMad,
        Number maxZeroMad,
        Number minZeroMad
    ) {
        var zeroExpected = DataTypeConverter.convert(0, type);
        var zeroValue = type == DataType.UNSIGNED_LONG ? NumericUtils.ZERO_AS_UNSIGNED_LONG : zeroExpected;
        var oneThousandValue = type == DataType.UNSIGNED_LONG
            ? NumericUtils.asLongUnsigned(BigInteger.valueOf(1000))
            : DataTypeConverter.convert(1000, type);

        var typeName = type.name().toLowerCase(Locale.ROOT);

        return List.of(
            new TestCaseSupplier(
                "mv_median_absolute_deviation(<max_" + typeName + ", min_" + typeName + ">)",
                List.of(type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(max, min), type, "field")),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    type,
                    equalTo(maxMinMad)
                )
            ),
            new TestCaseSupplier(
                "mv_median_absolute_deviation(<max_" + typeName + ", 0>)",
                List.of(type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(max, zeroValue), type, "field")),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    type,
                    equalTo(maxZeroMad)
                )
            ),
            new TestCaseSupplier(
                "mv_median_absolute_deviation(<min_" + typeName + ", 0>)",
                List.of(type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(min, zeroValue), type, "field")),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    type,
                    equalTo(minZeroMad)
                )
            ),
            new TestCaseSupplier(
                "mv_median_absolute_deviation(<max_" + typeName + ", max_" + typeName + ">)",
                List.of(type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(max, max), type, "field")),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    type,
                    equalTo(zeroExpected)
                )
            ),
            new TestCaseSupplier(
                "mv_median_absolute_deviation(<min_" + typeName + ", min_" + typeName + ">)",
                List.of(type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(min, min), type, "field")),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    type,
                    equalTo(zeroExpected)
                )
            ),
            new TestCaseSupplier(
                "mv_median_absolute_deviation(<min_" + typeName + ", 1000, 1000>)",
                List.of(type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(min, oneThousandValue, oneThousandValue), type, "field")),
                    "MvMedianAbsoluteDeviation[field=Attribute[channel=0]]",
                    type,
                    equalTo(zeroExpected)
                )
            )
        );
    }

    private static BigInteger calculateMedianAbsoluteDeviation(int size, Stream<BigInteger> valuesStream) {
        var values = valuesStream.sorted().toArray(BigInteger[]::new);
        int middle = size / 2;
        if (size % 2 == 1) {
            var median = values[middle];
            return Arrays.stream(values).map(bi -> bi.subtract(median).abs()).sorted().skip(middle).findFirst().orElseThrow();
        } else {
            var median = values[middle - 1].add(values[middle]).divide(BigInteger.valueOf(2));
            return Arrays.stream(values)
                .map(bi -> bi.subtract(median).abs())
                .sorted()
                .skip(middle - 1)
                .limit(2)
                .reduce(BigInteger.ZERO, BigInteger::add)
                .divide(BigInteger.valueOf(2));
        }
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvMedianAbsoluteDeviation(source, field);
    }
}
