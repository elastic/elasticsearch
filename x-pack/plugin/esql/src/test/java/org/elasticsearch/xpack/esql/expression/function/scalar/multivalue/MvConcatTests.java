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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvConcatTests extends AbstractScalarFunctionTestCase {
    public MvConcatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("mv_concat basic test", () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(
                        List.of(new BytesRef("foo"), new BytesRef("bar"), new BytesRef("baz")),
                        DataTypes.KEYWORD,
                        "field"
                    ),
                    new TestCaseSupplier.TypedData(new BytesRef(", "), DataTypes.KEYWORD, "delim")
                ),
                "MvConcat[field=Attribute[channel=0], delim=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef("foo, bar, baz"))
            );
        })));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvConcat(source, args.get(0), args.get(1));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    public void testNull() {
        // TODO: add these into the test parameters
        BytesRef foo = new BytesRef("foo");
        BytesRef bar = new BytesRef("bar");
        BytesRef delim = new BytesRef(";");
        Expression expression = buildFieldExpression(testCase);

        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(Arrays.asList(foo, bar), null))), 0), nullValue());
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(foo, null))), 0), nullValue());
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(null, null))), 0), nullValue());

        assertThat(
            toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(Arrays.asList(foo, bar), Arrays.asList(delim, bar)))), 0),
            nullValue()
        );
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(foo, Arrays.asList(delim, bar)))), 0), nullValue());
        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(null, Arrays.asList(delim, bar)))), 0), nullValue());

        assertThat(toJavaObject(evaluator(expression).get().eval(row(Arrays.asList(null, delim))), 0), nullValue());
    }
}
