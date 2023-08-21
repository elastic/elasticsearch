/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.VaragsTestCaseBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class GreatestTests extends AbstractFunctionTestCase {
    public GreatestTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = VaragsTestCaseBuilder.anyNullIsNull(
            t -> "Greatest" + t,
            s -> s.sorted(Comparator.<String>naturalOrder().reversed()).findFirst().get(),
            LongStream::max,
            IntStream::max
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                () -> new TestCase(
                    List.of(
                        new TypedData(new BytesRef("a"), DataTypes.KEYWORD, "a"),
                        new TypedData(new BytesRef("b"), DataTypes.KEYWORD, "b")
                    ),
                    "GreatestBytesRefEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]",
                    DataTypes.KEYWORD,
                    equalTo(new BytesRef("b"))
                )
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Greatest build(Source source, List<Expression> args) {
        return new Greatest(Source.EMPTY, args.get(0), args.subList(1, args.size()));
    }
}
