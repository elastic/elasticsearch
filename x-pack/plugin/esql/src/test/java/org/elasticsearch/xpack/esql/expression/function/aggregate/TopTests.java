/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TopTests extends AbstractAggregationTestCase {
    public TopTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        for (var limitCaseSupplier : TestCaseSupplier.intCases(1, 1000, false)) {
            for (String order : Arrays.asList("asc", "desc", null)) {
                Stream.of(
                    MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.dateCases(1, 1000),
                    MultiRowTestCaseSupplier.booleanCases(1, 1000),
                    MultiRowTestCaseSupplier.ipCases(1, 1000),
                    MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.KEYWORD),
                    MultiRowTestCaseSupplier.stringCases(1, 1000, DataType.TEXT)
                )
                    .flatMap(List::stream)
                    .map(fieldCaseSupplier -> TopTests.makeSupplier(fieldCaseSupplier, limitCaseSupplier, order, null))
                    .collect(Collectors.toCollection(() -> suppliers));
            }
        }

        for (var limitCaseSupplier : TestCaseSupplier.intCases(1, 1000, false)) {
            for (String order : Arrays.asList("asc", "desc")) {
                int rows = 100;
                List<TestCaseSupplier.TypedDataSupplier> fieldCaseSuppliers = Stream.of(
                    MultiRowTestCaseSupplier.intCases(rows, rows, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.longCases(rows, rows, Long.MIN_VALUE, Long.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.doubleCases(rows, rows, -Double.MAX_VALUE, Double.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.dateCases(rows, rows)
                ).flatMap(List::stream).toList();
                for (var fieldCaseSupplier : fieldCaseSuppliers) {
                    List<TestCaseSupplier.TypedDataSupplier> outputFieldCaseSuppliers = Stream.of(
                        MultiRowTestCaseSupplier.intCases(rows, rows, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                        MultiRowTestCaseSupplier.longCases(rows, rows, Long.MIN_VALUE, Long.MAX_VALUE, true),
                        MultiRowTestCaseSupplier.doubleCases(rows, rows, -Double.MAX_VALUE, Double.MAX_VALUE, true),
                        MultiRowTestCaseSupplier.dateCases(rows, rows)
                    ).flatMap(List::stream).toList();
                    for (var outputFieldCaseSupplier : outputFieldCaseSuppliers) {
                        if (fieldCaseSupplier.name().equals(outputFieldCaseSupplier.name())) {
                            continue;
                        }
                        suppliers.add(TopTests.makeSupplier(fieldCaseSupplier, limitCaseSupplier, order, outputFieldCaseSupplier));
                    }
                }
            }
        }

        suppliers.addAll(
            List.of(
                // Surrogates for cases where limit == 1
                new TestCaseSupplier(
                    List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(true, true, false), DataType.BOOLEAN, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxBoolean",
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5, 8, -2, 0, 200), DataType.INTEGER, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxInt",
                        DataType.INTEGER,
                        equalTo(200)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, -2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxLong",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5., 8., -2., 0., 200.), DataType.DOUBLE, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxDouble",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.DATETIME, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxLong",
                        DataType.DATETIME,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.IP, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(
                                List.of(
                                    new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1"))),
                                    new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))),
                                    new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::"))),
                                    new BytesRef(InetAddressPoint.encode(InetAddresses.forString("ffff::")))
                                ),
                                DataType.IP,
                                "field"
                            ),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxIp",
                        DataType.IP,
                        equalTo(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("ffff::"))))
                    )
                ),

                // Folding
                new TestCaseSupplier(
                    List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(true), DataType.BOOLEAN, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxBoolean",
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200), DataType.INTEGER, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxInt",
                        DataType.INTEGER,
                        equalTo(200)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxLong",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200.), DataType.DOUBLE, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxDouble",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.DATETIME, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxLong",
                        DataType.DATETIME,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.IP, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(
                                List.of(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")))),
                                DataType.IP,
                                "field"
                            ),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "MaxIp",
                        DataType.IP,
                        equalTo(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1"))))
                    )
                ),

                // Resolution errors
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(0, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Limit must be greater than 0 in [source], found [0]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(2, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("wrong-order"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Invalid order value in [source], expected [ASC, DESC] but got [wrong-order]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(null, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Limit must be a constant integer in [source], found [null]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(null, DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Invalid order value in [source], expected [ASC, DESC] but got [null]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD, DataType.BOOLEAN),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData("asc", DataType.KEYWORD, "order").forceLiteral(),
                            TestCaseSupplier.TypedData.multiRow(List.of(true, false), DataType.BOOLEAN, "outputField")
                        ),
                        "fourth argument of [source] must be [date or numeric except unsigned_long or counter types], "
                            + "found value [outputField] type [boolean]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData("asc", DataType.KEYWORD, "order").forceLiteral(),
                            TestCaseSupplier.TypedData.multiRow(List.of("a", "b"), DataType.KEYWORD, "outputField")
                        ),
                        "fourth argument of [source] must be [date or numeric except unsigned_long or counter types], "
                            + "found value [outputField] type [keyword]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD, DataType.IP),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData("asc", DataType.KEYWORD, "order").forceLiteral(),
                            TestCaseSupplier.TypedData.multiRow(List.of("192.168.0.1", "192.168.0.2"), DataType.IP, "outputField")
                        ),
                        "fourth argument of [source] must be [date or numeric except unsigned_long or counter types], "
                            + "found value [outputField] type [ip]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.KEYWORD, DataType.LONG),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(true, false), DataType.BOOLEAN, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData("asc", DataType.KEYWORD, "order").forceLiteral(),
                            TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L), DataType.LONG, "outputField")
                        ),
                        "when fourth argument is set, first argument of [source] must be "
                            + "[date or numeric except unsigned_long or counter types], found value [field] type [boolean]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.KEYWORD, DataType.INTEGER, DataType.KEYWORD, DataType.LONG),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of("a", "b"), DataType.KEYWORD, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData("asc", DataType.KEYWORD, "order").forceLiteral(),
                            TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L), DataType.LONG, "outputField")
                        ),
                        "when fourth argument is set, first argument of [source] must be "
                            + "[date or numeric except unsigned_long or counter types], found value [field] type [keyword]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.IP, DataType.INTEGER, DataType.KEYWORD, DataType.LONG),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of("192.168.0.1", "192.168.0.2"), DataType.IP, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData("asc", DataType.KEYWORD, "order").forceLiteral(),
                            TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L), DataType.LONG, "outputField")
                        ),
                        "when fourth argument is set, first argument of [source] must be "
                            + "[date or numeric except unsigned_long or counter types], found value [field] type [ip]"
                    )
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression field = args.get(0);
        Expression outputField = args.size() > 3 ? args.get(3) : null;
        if (field instanceof FieldAttribute f && outputField instanceof FieldAttribute of && f.fieldName().equals(of.fieldName())) {
            // In order to avoid passing the same field twice as two different FieldAttribute objects, we use `field` as the fourth argument
            // if `field`'s and `outputField`'s `fieldName` match. In such case TOP will use surrogate.
            return new Top(source, field, args.get(1), args.size() > 2 ? args.get(2) : null, field);
        } else {
            return new Top(source, field, args.get(1), args.size() > 2 ? args.get(2) : null, outputField);
        }
    }

    @SuppressWarnings("unchecked")
    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier limitCaseSupplier,
        String order,
        TestCaseSupplier.TypedDataSupplier outputFieldSupplier
    ) {
        boolean isAscending = order == null || order.equalsIgnoreCase("asc");
        boolean orderSupplied = order != null;
        boolean outputFieldSupplied = outputFieldSupplier != null;

        List<DataType> dataTypes = new ArrayList<>();
        dataTypes.add(fieldSupplier.type());
        dataTypes.add(DataType.INTEGER);
        if (orderSupplied) {
            dataTypes.add(DataType.KEYWORD);
        }
        if (outputFieldSupplied) {
            dataTypes.add(outputFieldSupplier.type());
        }

        DataType expectedType = outputFieldSupplied ? outputFieldSupplier.type() : fieldSupplier.type();

        return new TestCaseSupplier(fieldSupplier.name(), dataTypes, () -> {
            var fieldTypedData = fieldSupplier.get();
            var limitTypedData = limitCaseSupplier.get().forceLiteral();
            var limit = (int) limitTypedData.getValue();
            TestCaseSupplier.TypedData outputFieldTypedData;
            if (outputFieldSupplied) {
                outputFieldTypedData = outputFieldSupplier.get();
                assertThat(outputFieldTypedData.multiRowData(), hasSize(equalTo(fieldTypedData.multiRowData().size())));
            } else {
                outputFieldTypedData = fieldTypedData;
            }
            var comparator = Map.Entry.<Comparable<? super Comparable<?>>, Comparable<? super Comparable<?>>>comparingByKey()
                .thenComparing(Map.Entry::getValue);
            if (isAscending == false) {
                comparator = comparator.reversed();
            }
            List<?> expected = IntStream.range(0, fieldTypedData.multiRowData().size())
                .mapToObj(
                    i -> Map.<Comparable<? super Comparable<?>>, Comparable<? super Comparable<?>>>entry(
                        (Comparable<? super Comparable<?>>) fieldTypedData.multiRowData().get(i),
                        (Comparable<? super Comparable<?>>) outputFieldTypedData.multiRowData().get(i)
                    )
                )
                .sorted(comparator)
                .map(Map.Entry::getValue)
                .limit(limit)
                .toList();

            String baseName;
            if (limit != 1) {
                baseName = "Top";
            } else {
                // If the limit is 1 we rewrite TOP into MIN or MAX and never run our lovely TOP code.
                if (isAscending) {
                    baseName = "Min";
                } else {
                    baseName = "Max";
                }
            }

            List<TestCaseSupplier.TypedData> typedData = new ArrayList<>();
            typedData.add(fieldTypedData);
            typedData.add(limitTypedData);
            if (orderSupplied) {
                typedData.add(new TestCaseSupplier.TypedData(new BytesRef(order), DataType.KEYWORD, order + " order").forceLiteral());
            }
            if (outputFieldSupplied) {
                typedData.add(outputFieldTypedData);
            }

            return new TestCaseSupplier.TestCase(
                typedData,
                outputFieldSupplied && (fieldTypedData.name().equals(outputFieldTypedData.name()) == false)
                    ? standardAggregatorName(standardAggregatorName(baseName, fieldTypedData.type()), outputFieldTypedData.type())
                    : standardAggregatorName(baseName, fieldTypedData.type()),
                expectedType,
                equalTo(expected.size() == 1 ? expected.get(0) : expected)
            );
        });
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // TODO: This aggregation doesn't serialize the Source, and must be fixed.
        return expression;
    }
}
