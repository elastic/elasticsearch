/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.VaragsTestCaseBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class LeastTests extends AbstractFunctionTestCase {
    public LeastTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        VaragsTestCaseBuilder builder = new VaragsTestCaseBuilder(t -> "Least" + t);
        builder.expectedEvaluatorValueWrap(e -> "MvMin[field=" + e + "]");
        builder.expectFlattenedString(s -> s.sorted().findFirst());
        builder.expectFlattenedBoolean(s -> s.sorted().findFirst());
        builder.expectFlattenedInt(IntStream::min);
        builder.expectFlattenedLong(LongStream::min);
        List<TestCaseSupplier> suppliers = builder.suppliers();
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataTypes.KEYWORD, DataTypes.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("a"), DataTypes.KEYWORD, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("b"), DataTypes.KEYWORD, "b")
                    ),
                    "LeastBytesRefEvaluator[values=[MvMin[field=Attribute[channel=0]], MvMin[field=Attribute[channel=1]]]]",
                    DataTypes.KEYWORD,
                    equalTo(new BytesRef("a"))
                )
            )
        );
        return parameterSuppliersFromTypedData(anyNullIsNull(false, suppliers));
    }

    @Override
    protected Least build(Source source, List<Expression> args) {
        return new Least(source, args.get(0), args.subList(1, args.size()));
    }
}
