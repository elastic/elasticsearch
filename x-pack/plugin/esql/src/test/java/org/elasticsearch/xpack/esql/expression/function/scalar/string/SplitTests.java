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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
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
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType sType : DataType.stringTypes()) {
            for (DataType dType : DataType.stringTypes()) {
                suppliers.add(new TestCaseSupplier("split test " + sType.toString() + " " + dType.toString(), List.of(sType, dType), () -> {
                    String delimiter = randomAlphaOfLength(1);
                    List<BytesRef> strings = IntStream.range(0, between(1, 5))
                        .mapToObj(i -> randomValueOtherThanMany(s -> s.contains(delimiter), () -> randomAlphaOfLength(4)))
                        .map(BytesRef::new)
                        .collect(Collectors.toList());
                    String str = strings.stream().map(BytesRef::utf8ToString).collect(joining(delimiter));
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef(str), sType, "str"),
                            new TestCaseSupplier.TypedData(new BytesRef(delimiter), dType, "delim")
                        ),
                        "SplitVariableEvaluator[str=Attribute[channel=0], delim=Attribute[channel=1]]",
                        DataType.KEYWORD,
                        equalTo(strings.size() == 1 ? strings.get(0) : strings)
                    );
                }));
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Split(source, args.get(0), args.get(1));
    }

    public void testConstantDelimiter() {
        DriverContext driverContext = driverContext();
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Split(Source.EMPTY, field("str", DataType.KEYWORD), new Literal(Source.EMPTY, new BytesRef(":"), DataType.KEYWORD))
            ).get(driverContext)
        ) {
            /*
             * 58 is ascii for : and appears in the toString below. We don't convert the delimiter to a
             * string because we aren't really sure it's printable. It could be a tab or a bell or some
             * garbage.
             */
            assert ':' == 58;
            assertThat(eval.toString(), equalTo("SplitSingleByteEvaluator[str=Attribute[channel=0], delim=58]"));
            BlockFactory blockFactory = driverContext.blockFactory();
            Page page = new Page(blockFactory.newConstantBytesRefBlockWith(new BytesRef("foo:bar"), 1));
            try (Block block = eval.eval(page)) {
                assertThat(toJavaObject(block, 0), equalTo(List.of(new BytesRef("foo"), new BytesRef("bar"))));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testTooLongConstantDelimiter() {
        String delimiter = randomAlphaOfLength(2);
        DriverContext driverContext = driverContext();
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> evaluator(
                new Split(
                    Source.EMPTY,
                    field("str", DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef(delimiter), DataType.KEYWORD)
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), equalTo("delimiter must be single byte for now"));
    }
}
