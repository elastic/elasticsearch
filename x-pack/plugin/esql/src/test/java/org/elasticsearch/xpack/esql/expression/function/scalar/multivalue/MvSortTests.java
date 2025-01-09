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

import java.util.ArrayList;
import java.util.Collections;
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
        for (DataType orderType : DataType.stringTypes()) {
            booleans(suppliers, orderType);
            ints(suppliers, orderType);
            longs(suppliers, orderType);
            doubles(suppliers, orderType);
            bytesRefs(suppliers, orderType);
            nulls(suppliers, orderType);
        }

        List<TestCaseSupplier> extra = new ArrayList<>();
        for (TestCaseSupplier s : suppliers) {
            extra.add(new TestCaseSupplier("null <" + s.types().get(0) + ">, <" + s.types().get(1) + ">", s.types(), () -> {
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
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSort(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    private static void booleans(List<TestCaseSupplier> suppliers, DataType orderType) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, orderType), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortBoolean[field=Attribute[channel=0], order=true]",
                DataType.BOOLEAN,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers, DataType orderType) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, orderType), () -> {
            List<Integer> field = randomList(1, 10, () -> randomInt());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortInt[field=Attribute[channel=0], order=false]",
                DataType.INTEGER,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers, DataType orderType) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG, orderType), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.LONG, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortLong[field=Attribute[channel=0], order=true]",
                DataType.LONG,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.DATETIME, orderType), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DATETIME, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortLong[field=Attribute[channel=0], order=false]",
                DataType.DATETIME,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.DATE_NANOS, orderType), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DATE_NANOS, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortLong[field=Attribute[channel=0], order=false]",
                DataType.DATE_NANOS,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers, DataType orderType) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE, orderType), () -> {
            List<Double> field = randomList(1, 10, () -> randomDouble());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DOUBLE, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortDouble[field=Attribute[channel=0], order=true]",
                DataType.DOUBLE,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers, DataType orderType) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.KEYWORD, orderType), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.KEYWORD).value());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.KEYWORD, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortBytesRef[field=Attribute[channel=0], order=false]",
                DataType.KEYWORD,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.TEXT, orderType), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.TEXT).value());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.TEXT, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortBytesRef[field=Attribute[channel=0], order=true]",
                DataType.TEXT,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.SEMANTIC_TEXT, orderType), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.SEMANTIC_TEXT).value());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.SEMANTIC_TEXT, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortBytesRef[field=Attribute[channel=0], order=true]",
                DataType.SEMANTIC_TEXT,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.IP, orderType), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.IP, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortBytesRef[field=Attribute[channel=0], order=false]",
                DataType.IP,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.VERSION, orderType), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.VERSION, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
                ),
                "MvSortBytesRef[field=Attribute[channel=0], order=true]",
                DataType.VERSION,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));
    }

    private static void nulls(List<TestCaseSupplier> suppliers, DataType orderType) {
        suppliers.add(new TestCaseSupplier("<null>, <keyword>", List.of(DataType.NULL, orderType), () -> {
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(null, DataType.NULL, "field"),
                    new TestCaseSupplier.TypedData(order, orderType, "order").forceLiteral()
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
