/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvSortTests extends AbstractScalarFunctionTestCase {
    public MvSortTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        booleans(suppliers);
        ints(suppliers);
        longs(suppliers);
        doubles(suppliers);
        bytesRefs(suppliers);
        nulls(suppliers);

        suppliers = anyNullIsNull(
            suppliers,
            (nullPosition, nullValueDataType, original) -> nullPosition == 0 ? nullValueDataType : original.expectedType(),
            (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSort(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    private static void booleans(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.KEYWORD), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(order, DataType.KEYWORD, "order").forceLiteral()
                ),
                "MvSortBoolean[field=Attribute[channel=0], order=true]",
                DataType.BOOLEAN,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DataType.KEYWORD), () -> {
            List<Integer> field = randomList(1, 10, () -> randomInt());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(order, DataType.KEYWORD, "order").forceLiteral()
                ),
                "MvSortInt[field=Attribute[channel=0], order=false]",
                DataType.INTEGER,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG, DataType.KEYWORD), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.LONG, "field"),
                    new TestCaseSupplier.TypedData(order, DataType.KEYWORD, "order").forceLiteral()
                ),
                "MvSortLong[field=Attribute[channel=0], order=true]",
                DataType.LONG,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.DATETIME, DataType.KEYWORD), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DATETIME, "field"),
                    new TestCaseSupplier.TypedData(order, DataType.KEYWORD, "order").forceLiteral()
                ),
                "MvSortLong[field=Attribute[channel=0], order=false]",
                DataType.DATETIME,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.DATE_NANOS, DataType.KEYWORD), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DATE_NANOS, "field"),
                    new TestCaseSupplier.TypedData(order, DataType.KEYWORD, "order").forceLiteral()
                ),
                "MvSortLong[field=Attribute[channel=0], order=false]",
                DataType.DATE_NANOS,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE, DataType.KEYWORD), () -> {
            List<Double> field = randomList(1, 10, () -> randomDouble());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DOUBLE, "field"),
                    new TestCaseSupplier.TypedData(order, DataType.KEYWORD, "order").forceLiteral()
                ),
                "MvSortDouble[field=Attribute[channel=0], order=true]",
                DataType.DOUBLE,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        for (DataType type : List.of(DataType.KEYWORD, DataType.TEXT, DataType.IP, DataType.VERSION)) {
            suppliers.add(new TestCaseSupplier(List.of(type, DataType.KEYWORD), () -> {
                List<BytesRef> field = randomList(1, 10, () -> (BytesRef) randomLiteral(type).value());
                boolean order = randomBoolean();
                var sortedData = field.stream().sorted(order ? Comparator.naturalOrder() : Comparator.reverseOrder()).toList();
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, type, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef(order ? "ASC" : "DESC"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "MvSortBytesRef[field=Attribute[channel=0], order=" + order + "]",
                    type,
                    equalTo(sortedData.size() == 1 ? sortedData.get(0) : sortedData)
                );
            }));
        }
    }

    private static void nulls(List<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> extra = new ArrayList<>();
        for (TestCaseSupplier s : suppliers) {
            extra.add(new TestCaseSupplier("null <" + s.types().get(0) + ">, <keyword>", s.types(), () -> {
                TestCaseSupplier.TestCase delegate = s.get();
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(null, s.types().get(0), "field"), delegate.getData().get(1)),
                    delegate.evaluatorToString(),
                    delegate.expectedType(),
                    nullValue()
                );
            }));
        }
        suppliers.addAll(extra);
        suppliers.add(new TestCaseSupplier("<null>, <keyword>", List.of(DataType.NULL, DataType.KEYWORD), () -> {
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(null, DataType.NULL, "field"),
                    new TestCaseSupplier.TypedData(order, DataType.KEYWORD, "order").forceLiteral()
                ),
                equalTo("LiteralsEvaluator[lit=null]"),
                DataType.NULL,
                nullValue()
            );
        }));
    }

    public void testInvalidOrder() {
        // TODO move to parameters
        String invalidOrder = randomAlphaOfLength(10);
        DriverContext driverContext = driverContext();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> evaluator(
                new MvSort(
                    Source.EMPTY,
                    field("str", DataType.DATETIME),
                    new Literal(Source.EMPTY, new BytesRef(invalidOrder), DataType.KEYWORD)
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), equalTo("Invalid order value in [], expected one of [ASC, DESC] but got [" + invalidOrder + "]"));
    }
}
