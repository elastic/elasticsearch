/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import com.carrotsearch.randomizedtesting.annotations.Name;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;


public class RerankFunctionTests extends AbstractFunctionTestCase {
    public RerankFunctionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier.TypedData> fixedParamsValues = List.of(
            new TestCaseSupplier.TypedData(new BytesRef("field value"), KEYWORD, "field"),
            new TestCaseSupplier.TypedData(new BytesRef("query"), KEYWORD, "query")
        );

        var suppliers = Stream.of(
            new TestCaseSupplier.TypedData(null, UNSUPPORTED, "options").forceLiteral(),
            new TestCaseSupplier.TypedData(new MapExpression(EMPTY, List.of()), UNSUPPORTED, "options").forceLiteral(),
            new TestCaseSupplier.TypedData(
                new MapExpression(EMPTY, List.of(new Literal(EMPTY, "inference_id", KEYWORD), new Literal(EMPTY, "inference_id", KEYWORD))),
                UNSUPPORTED,
                "options"
            ).forceLiteral()
        ).map(option -> new TestCaseSupplier(
            List.of(KEYWORD, KEYWORD, UNSUPPORTED),
            () -> new TestCaseSupplier.TestCase(Stream.concat(fixedParamsValues.stream(), Stream.of(option)).toList())
        )).toList();

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RerankFunction(source, args.get(0), args.get(1), args.get(2));
    }
}
