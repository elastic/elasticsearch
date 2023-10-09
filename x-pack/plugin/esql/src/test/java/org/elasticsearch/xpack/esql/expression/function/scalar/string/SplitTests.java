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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class SplitTests extends AbstractScalarFunctionTestCase {
    public SplitTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("split basic test", () -> {
            String delimiter = randomAlphaOfLength(1);
            List<BytesRef> strings = IntStream.range(0, between(1, 5))
                .mapToObj(i -> randomValueOtherThanMany(s -> s.contains(delimiter), () -> randomAlphaOfLength(4)))
                .map(BytesRef::new)
                .collect(Collectors.toList());
            String str = strings.stream().map(BytesRef::utf8ToString).collect(joining(delimiter));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(delimiter), DataTypes.KEYWORD, "delim")
                ),
                "SplitVariableEvaluator[str=Attribute[channel=0], delim=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(strings.size() == 1 ? strings.get(0) : strings)
            );
        })));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    private Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        String delim = ((BytesRef) typedData.get(1).data()).utf8ToString();
        List<BytesRef> split = Arrays.stream(str.split(Pattern.quote(delim))).map(BytesRef::new).toList();
        return equalTo(split.size() == 1 ? split.get(0) : split);
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Split(source, args.get(0), args.get(1));
    }

    public void testConstantDelimiter() {
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Split(Source.EMPTY, field("str", DataTypes.KEYWORD), new Literal(Source.EMPTY, new BytesRef(":"), DataTypes.KEYWORD))
            ).get(driverContext())
        ) {
            /*
             * 58 is ascii for : and appears in the toString below. We don't convert the delimiter to a
             * string because we aren't really sure it's printable. It could be a tab or a bell or some
             * garbage.
             */
            assert ':' == 58;
            assertThat(eval.toString(), equalTo("SplitSingleByteEvaluator[str=Attribute[channel=0], delim=58]"));
            try (Block.Ref ref = eval.eval(new Page(BytesRefBlock.newConstantBlockWith(new BytesRef("foo:bar"), 1)))) {
                assertThat(toJavaObject(ref.block(), 0), equalTo(List.of(new BytesRef("foo"), new BytesRef("bar"))));
            }
        }
    }
}
