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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link Locate} function.
 */
public class LocateTests extends AbstractFunctionTestCase {
    public LocateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(supplier("keywords", DataTypes.KEYWORD, () -> randomAlphaOfLength(10), () -> randomAlphaOfLength(5)));
        suppliers.add(supplier("texts", DataTypes.TEXT, () -> randomAlphaOfLength(10), () -> randomAlphaOfLength(5)));
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(false, suppliers)));
    }

    public void testToString() {
        assertThat(
            evaluator(new Locate(Source.EMPTY, field("str", DataTypes.KEYWORD), field("substr", DataTypes.KEYWORD))).get(driverContext())
                .toString(),
            equalTo("LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1]]")
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Locate(source, args.get(0), args.get(1));
    }

    public void testPrefixString() {
        assertThat(process("a tiger", "a t"), equalTo(0));
        assertThat(process("a tiger", "a t"), equalTo(0));
    }

    private Integer process(String str, String substr) {
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Locate(Source.EMPTY, field("str", DataTypes.KEYWORD), field("substr", DataTypes.KEYWORD))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str), new BytesRef(substr))))
        ) {
            return block.isNull(0) ? Integer.valueOf(-1) : ((Integer) toJavaObject(block, 0));
        }
    }

    private static TestCaseSupplier supplier(
        String name,
        DataType type,
        Supplier<String> strValueSupplier,
        Supplier<String> substrValueSupplier
    ) {
        return new TestCaseSupplier(name, List.of(type, type), () -> {
            List<TestCaseSupplier.TypedData> values = new ArrayList<>();
            String expectedToString = "LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1]]";

            String value = strValueSupplier.get();
            values.add(new TestCaseSupplier.TypedData(new BytesRef(value), type, "0"));

            String substrValue = substrValueSupplier.get();
            values.add(new TestCaseSupplier.TypedData(new BytesRef(substrValue), type, "1"));

            int expectedValue = value.indexOf(substrValue);
            return new TestCaseSupplier.TestCase(values, expectedToString, DataTypes.INTEGER, equalTo(expectedValue));
        });
    }
}
