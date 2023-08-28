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
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class LeftTests extends AbstractScalarFunctionTestCase {
    public LeftTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(new TestCaseSupplier("long", () -> {
            int length = between(1, 10);
            String text = randomAlphaOfLength(10);
            return new TestCase(
                List.of(new TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"), new TypedData(length, DataTypes.INTEGER, "length")),
                "LeftEvaluator[out=[], str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(text.substring(0, length)))
            );
        }));
        suppliers.add(new TestCaseSupplier("short", () -> {
            int length = between(2, 10);
            String text = randomAlphaOfLength(1);
            return new TestCase(
                List.of(new TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"), new TypedData(length, DataTypes.INTEGER, "length")),
                "LeftEvaluator[out=[], str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(text))
            );
        }));
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Left(source, args.get(0), args.get(1));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(integers()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    public Matcher<Object> resultsMatcher(List<TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        int length = (Integer) typedData.get(1).data();
        return equalTo(new BytesRef(str.substring(0, length)));
    }

    public void testReasonableLength() {
        assertThat(process("a fox call", 5), equalTo("a fox"));
    }

    public void testMassiveLength() {
        assertThat(process("a fox call", 10), equalTo("a fox call"));
    }

    public void testNegativeLength() {
        assertThat(process("a fox call", -1), equalTo(""));
    }

    public void testUnicode() {
        final String s = "a\ud83c\udf09tiger";
        assert s.codePointCount(0, s.length()) == 7;
        assertThat(process(s, 2), equalTo("a\ud83c\udf09"));
    }

    private String process(String str, int length) {
        Block result = evaluator(
            new Left(Source.EMPTY, field("str", DataTypes.KEYWORD), new Literal(Source.EMPTY, length, DataTypes.INTEGER))
        ).get().eval(row(List.of(new BytesRef(str))));
        if (null == result) {
            return null;
        }
        BytesRef resultByteRef = ((BytesRef) toJavaObject(result, 0));
        return resultByteRef == null ? null : resultByteRef.utf8ToString();
    }
}
