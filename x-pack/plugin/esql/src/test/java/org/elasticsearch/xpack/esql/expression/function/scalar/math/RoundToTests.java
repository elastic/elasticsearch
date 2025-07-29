/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class RoundToTests extends AbstractScalarFunctionTestCase {
    public RoundToTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (int p = 1; p < 20; p++) {
            int points = p;
            suppliers.add(
                doubles(
                    "<double, " + points + " doubles>",
                    DataType.DOUBLE,
                    () -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true),
                    IntStream.range(0, points).mapToObj(i -> DataType.DOUBLE).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true)).toList()
                )
            );
            suppliers.add(
                doubles(
                    "<double, " + points + " longs>",
                    DataType.DOUBLE,
                    () -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true),
                    IntStream.range(0, points).mapToObj(i -> DataType.LONG).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> (double) randomLong()).toList()
                )
            );
            suppliers.add(
                doubles(
                    "<double, " + points + " ints>",
                    DataType.DOUBLE,
                    () -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true),
                    IntStream.range(0, points).mapToObj(i -> DataType.INTEGER).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> (double) randomInt()).toList()
                )
            );
            suppliers.add(
                doubles(
                    "<long, " + points + " doubles>",
                    DataType.LONG,
                    ESTestCase::randomLong,
                    IntStream.range(0, points).mapToObj(i -> DataType.DOUBLE).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true)).toList()
                )
            );
            suppliers.add(
                doubles(
                    "<int, " + points + " doubles>",
                    DataType.INTEGER,
                    ESTestCase::randomInt,
                    IntStream.range(0, points).mapToObj(i -> DataType.DOUBLE).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true)).toList()
                )
            );
            suppliers.add(
                longs(
                    "<long, " + points + " longs>",
                    DataType.LONG,
                    ESTestCase::randomLong,
                    IntStream.range(0, points).mapToObj(i -> DataType.LONG).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomLong()).toList()
                )
            );
            suppliers.add(
                longs(
                    "<int, " + points + " longs>",
                    DataType.INTEGER,
                    ESTestCase::randomInt,
                    IntStream.range(0, points).mapToObj(i -> DataType.LONG).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomLong()).toList()
                )
            );
            suppliers.add(
                longs(
                    "<date, " + points + " dates>",
                    DataType.DATETIME,
                    ESTestCase::randomMillisUpToYear9999,
                    IntStream.range(0, points).mapToObj(i -> DataType.DATETIME).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomMillisUpToYear9999()).toList()
                )
            );
            suppliers.add(
                longs(
                    "<date_nanos, " + points + " date_nanos>",
                    DataType.DATE_NANOS,
                    () -> randomLongBetween(0, Long.MAX_VALUE),
                    IntStream.range(0, points).mapToObj(i -> DataType.DATE_NANOS).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomLongBetween(0, Long.MAX_VALUE)).toList()
                )
            );
            suppliers.add(
                longs(
                    "<long, " + points + " ints>",
                    DataType.LONG,
                    ESTestCase::randomLong,
                    IntStream.range(0, points).mapToObj(i -> DataType.INTEGER).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> (long) randomInt()).toList()
                )
            );
            suppliers.add(
                ints(
                    "<int, " + points + " ints>",
                    DataType.INTEGER,
                    ESTestCase::randomInt,
                    IntStream.range(0, points).mapToObj(i -> DataType.INTEGER).toList(),
                    () -> IntStream.range(0, points).mapToObj(i -> randomInt()).toList()
                )
            );
        }
        suppliers.add(supplier(1.0, 0.0, 0.0, 100.0));
        suppliers.add(supplier(1.0, 1.0, 0.0, 1.0, 100.0));
        suppliers.add(supplier(0.5, 0.0, 0.0, 1.0, 100.0));
        suppliers.add(supplier(1.5, 1.0, 0.0, 1.0, 100.0));
        suppliers.add(supplier(200, 100, 0.0, 1.0, 100.0));

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(
            (int nullPosition, DataType nullValueDataType, TestCaseSupplier.TestCase original) -> {
                if (nullValueDataType != DataType.NULL) {
                    return original.expectedType();
                }
                List<DataType> types = original.getData().stream().map(TestCaseSupplier.TypedData::type).collect(Collectors.toList());
                types.set(nullPosition, DataType.NULL);
                return expectedType(types);
            },
            (int nullPosition, TestCaseSupplier.TypedData nullData, Matcher<String> original) -> {
                if (nullPosition == 0) {
                    return original;
                }
                return equalTo("LiteralsEvaluator[lit=null]");
            },
            randomizeBytesRefsOffset(suppliers)
        );
    }

    private static TestCaseSupplier supplier(double f, double expected, double... points) {
        StringBuilder name = new StringBuilder("round(");
        name.append(f);
        for (double p : points) {
            name.append(", ").append(p);
        }
        name.append(") -> ").append(expected);
        return supplier(
            name.toString(),
            DataType.DOUBLE,
            () -> f,
            IntStream.range(0, points.length).mapToObj(i -> DataType.DOUBLE).toList(),
            () -> Arrays.stream(points).boxed().toList(),
            (value, de) -> expected
        );
    }

    private static TestCaseSupplier doubles(
        String name,
        DataType fieldType,
        Supplier<Number> fieldSupplier,
        List<DataType> pointsTypes,
        Supplier<List<Double>> pointsSupplier
    ) {
        return supplier(name, fieldType, fieldSupplier, pointsTypes, pointsSupplier, (f, p) -> {
            double max = p.stream().mapToDouble(d -> d).min().getAsDouble();
            for (double d : p) {
                if (d > max && f.doubleValue() > d) {
                    max = d;
                }
            }
            return max;
        });
    }

    private static TestCaseSupplier longs(
        String name,
        DataType fieldType,
        Supplier<Number> fieldSupplier,
        List<DataType> pointsTypes,
        Supplier<List<Long>> pointsSupplier
    ) {
        return supplier(name, fieldType, fieldSupplier, pointsTypes, pointsSupplier, (f, p) -> {
            long max = p.stream().mapToLong(l -> l).min().getAsLong();
            for (long l : p) {
                if (l > max && f.doubleValue() > l) {
                    max = l;
                }
            }
            return max;
        });
    }

    private static TestCaseSupplier ints(
        String name,
        DataType fieldType,
        Supplier<Number> fieldSupplier,
        List<DataType> pointsTypes,
        Supplier<List<Integer>> pointsSupplier
    ) {
        return supplier(name, fieldType, fieldSupplier, pointsTypes, pointsSupplier, (f, p) -> {
            int max = p.stream().mapToInt(i -> i).min().getAsInt();
            for (int l : p) {
                if (l > max && f.doubleValue() > l) {
                    max = l;
                }
            }
            return max;
        });
    }

    private static <P> TestCaseSupplier supplier(
        String name,
        DataType fieldType,
        Supplier<Number> fieldSupplier,
        List<DataType> pointsTypes,
        Supplier<List<P>> pointsSupplier,
        BiFunction<Number, List<P>, Number> expected
    ) {
        List<DataType> types = new ArrayList<>(pointsTypes.size() + 1);
        types.add(fieldType);
        types.addAll(pointsTypes);
        return new TestCaseSupplier(name, types, () -> {
            Number field = fieldSupplier.get();
            List<P> points = pointsSupplier.get();

            List<TestCaseSupplier.TypedData> params = new ArrayList<>(1 + points.size());
            params.add(new TestCaseSupplier.TypedData(field, fieldType, "field"));
            for (int i = 0; i < points.size(); i++) {
                params.add(new TestCaseSupplier.TypedData(points.get(i), pointsTypes.get(i), "point" + i).forceLiteral());
            }

            DataType expectedType = expectedType(types);
            String type = switch (expectedType) {
                case DOUBLE -> "Double";
                case INTEGER -> "Int";
                case DATETIME, DATE_NANOS, LONG -> "Long";
                default -> throw new UnsupportedOperationException();
            };
            Matcher<String> expectedEvaluatorName = startsWith("RoundTo" + type + specialization(points.size()) + "Evaluator");
            return new TestCaseSupplier.TestCase(params, expectedEvaluatorName, expectedType, equalTo(expected.apply(field, points)));
        });
    }

    private static String specialization(int pointsSize) {
        if (pointsSize < 5) {
            return Integer.toString(pointsSize);
        }
        if (pointsSize < 11) {
            return "LinearSearch";
        }
        return "BinarySearch";
    }

    private static DataType expectedType(List<DataType> types) {
        if (types.stream().anyMatch(t -> t == DataType.DOUBLE)) {
            return DataType.DOUBLE;
        }
        if (types.stream().anyMatch(t -> t == DataType.LONG)) {
            return DataType.LONG;
        }
        if (types.stream().anyMatch(t -> t == DataType.INTEGER)) {
            return DataType.INTEGER;
        }
        if (types.stream().anyMatch(t -> t == DataType.DATETIME)) {
            return DataType.DATETIME;
        }
        if (types.stream().anyMatch(t -> t == DataType.DATE_NANOS)) {
            return DataType.DATE_NANOS;
        }
        throw new UnsupportedOperationException("can't build expected types for " + types);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RoundTo(source, args.get(0), args.subList(1, args.size()));
    }
}
