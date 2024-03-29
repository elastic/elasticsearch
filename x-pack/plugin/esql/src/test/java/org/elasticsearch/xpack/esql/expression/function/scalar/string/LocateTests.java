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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link Locate} function.
 */
public class LocateTests extends AbstractScalarFunctionTestCase {
    public LocateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Locate basic test with keyword", () -> {
            String str = randomAlphaOfLength(10);
            String substr = str.substring(5);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(substr), DataTypes.KEYWORD, "substr")
                ),
                "LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1]]",
                DataTypes.INTEGER,
                equalTo(5)
            );
        }), new TestCaseSupplier("Locate basic test with text", () -> {
            String str = randomAlphaOfLength(10);
            String substr = str.substring(5);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataTypes.TEXT, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(substr), DataTypes.TEXT, "substr")
                ),
                "LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1]]",
                DataTypes.INTEGER,
                equalTo(5)
            );
        })));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.INTEGER;
    }

    public Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        String substr = ((BytesRef) typedData.get(1).data()).utf8ToString();
        return equalTo(str.indexOf(substr));
    }

    public void testToString() {
        assertThat(
            evaluator(new Locate(Source.EMPTY, field("str", DataTypes.KEYWORD), field("substr", DataTypes.KEYWORD))).get(driverContext())
                .toString(),
            equalTo("LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1]]")
        );
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
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

}
