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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvSortTests extends AbstractFunctionTestCase {
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
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSort(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    /**
     * Override to create the second argument as a Literal instead of a FieldAttribute.
     */
    @Override
    protected Expression buildFieldExpression(TestCaseSupplier.TestCase testCase) {
        List<Expression> args = new ArrayList<>(2);
        List<TestCaseSupplier.TypedData> data = testCase.getData();
        args.add(AbstractFunctionTestCase.field(data.get(0).name(), data.get(0).type()));
        args.add(new Literal(Source.synthetic(data.get(1).name()), data.get(1).data(), data.get(1).type()));
        return build(testCase.getSource(), args);
    }

    /**
     * Override to create the second argument as a Literal instead of a FieldAttribute.
     */
    @Override
    protected Expression buildDeepCopyOfFieldExpression(TestCaseSupplier.TestCase testCase) {
        List<Expression> args = new ArrayList<>(2);
        List<TestCaseSupplier.TypedData> data = testCase.getData();
        args.add(AbstractFunctionTestCase.deepCopyOfField(data.get(0).name(), data.get(0).type()));
        args.add(new Literal(Source.synthetic(data.get(1).name()), data.get(1).data(), data.get(1).type()));
        return build(testCase.getSource(), args);
    }

    private static void booleans(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.BOOLEAN, DataTypes.KEYWORD), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.BOOLEAN + "[field=Attribute[channel=0], order=true]",
                DataTypes.BOOLEAN,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));

    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.INTEGER, DataTypes.KEYWORD), () -> {
            List<Integer> field = randomList(1, 10, () -> randomInt());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.INT + "[field=Attribute[channel=0], order=false]",
                DataTypes.INTEGER,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.LONG, DataTypes.KEYWORD), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.LONG, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.LONG + "[field=Attribute[channel=0], order=true]",
                DataTypes.LONG,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.DATETIME, DataTypes.KEYWORD), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.DATETIME, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.LONG + "[field=Attribute[channel=0], order=false]",
                DataTypes.DATETIME,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.DOUBLE, DataTypes.KEYWORD), () -> {
            List<Double> field = randomList(1, 10, () -> randomDouble());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.DOUBLE, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.DOUBLE + "[field=Attribute[channel=0], order=true]",
                DataTypes.DOUBLE,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.KEYWORD, DataTypes.KEYWORD), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.KEYWORD).value());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.KEYWORD, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.BYTES_REF + "[field=Attribute[channel=0], order=false]",
                DataTypes.KEYWORD,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.TEXT, DataTypes.KEYWORD), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.TEXT).value());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.TEXT, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.BYTES_REF + "[field=Attribute[channel=0], order=true]",
                DataTypes.TEXT,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.IP, DataTypes.KEYWORD), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.IP).value());
            BytesRef order = new BytesRef("DESC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.IP, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.BYTES_REF + "[field=Attribute[channel=0], order=false]",
                DataTypes.IP,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted(Collections.reverseOrder()).toList())
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.VERSION, DataTypes.KEYWORD), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.VERSION).value());
            BytesRef order = new BytesRef("ASC");
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.VERSION, "field"),
                    new TestCaseSupplier.TypedData(order, DataTypes.KEYWORD, "order")
                ),
                "MvSort" + ElementType.BYTES_REF + "[field=Attribute[channel=0], order=true]",
                DataTypes.VERSION,
                equalTo(field.size() == 1 ? field.iterator().next() : field.stream().sorted().toList())
            );
        }));
    }

    @Override
    public void testSimpleWithNulls() {
        assumeFalse("test case is invalid", false);
    }
}
