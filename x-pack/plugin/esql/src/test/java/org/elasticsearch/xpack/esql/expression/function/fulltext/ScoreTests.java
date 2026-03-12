/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("score")
public class ScoreTests extends AbstractMatchFullTextFunctionTests {

    public ScoreTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(
            new TestCaseSupplier(
                List.of(BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(randomBoolean(), BOOLEAN, "query")),
                    equalTo("ScoreEvaluator" + ScoreTests.class.getSimpleName()),
                    DOUBLE,
                    equalTo(true)
                )
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Score(source, args.getFirst());
    }

}
