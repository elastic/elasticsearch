/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;
import static org.hamcrest.Matchers.equalTo;

public class KnnTests extends AbstractFunctionTestCase {

    public KnnTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addFunctionNamedParams(testCaseSuppliers()));
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(
            TestCaseSupplier.testCaseSupplier(
                new TestCaseSupplier.TypedDataSupplier("dense_vector field", KnnTests::randomDenseVector, DENSE_VECTOR),
                new TestCaseSupplier.TypedDataSupplier("query", KnnTests::randomDenseVector, DENSE_VECTOR, true),
                (d1, d2) -> equalTo("string"),
                BOOLEAN,
                (o1, o2) -> true
            )
        );

        return suppliers;
    }

    private static List<Float> randomDenseVector() {
        int dimensions = randomIntBetween(64, 128);
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimensions; i++) {
            vector.add(randomFloat());
        }
        return vector;
    }

    /**
     * Adds function named parameters to all the test case suppliers provided
     */
    private static List<TestCaseSupplier> addFunctionNamedParams(List<TestCaseSupplier> suppliers) {
        // TODO get to a common class with MatchTests
        List<TestCaseSupplier> result = new ArrayList<>();
        for (TestCaseSupplier supplier : suppliers) {
            List<DataType> dataTypes = new ArrayList<>(supplier.types());
            dataTypes.add(UNSUPPORTED);
            result.add(new TestCaseSupplier(supplier.name() + ", options", dataTypes, () -> {
                List<TestCaseSupplier.TypedData> values = new ArrayList<>(supplier.get().getData());
                values.add(
                    new TestCaseSupplier.TypedData(
                        new MapExpression(Source.EMPTY, List.of(new Literal(Source.EMPTY, randomAlphaOfLength(10), KEYWORD))),
                        UNSUPPORTED,
                        "options"
                    ).forceLiteral()
                );

                return new TestCaseSupplier.TestCase(values, equalTo("KnnEvaluator"), BOOLEAN, equalTo(true));
            }));
        }
        return result;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Knn knn = new Knn(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
        // We need to add the QueryBuilder to the match expression, as it is used to implement equals() and hashCode() and
        // thus test the serialization methods. But we can only do this if the parameters make sense .
        if (args.get(0) instanceof FieldAttribute && args.get(1).foldable()) {
            QueryBuilder queryBuilder = TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, knn).toQueryBuilder();
            knn = (Knn) knn.replaceQueryBuilder(queryBuilder);
        }
        return knn;
    }

    /**
     * Copy of the overridden method that doesn't check for children size, as the {@code options} child isn't serialized in Match.
     */
    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        Expression newExpression = serializeDeserialize(
            expression,
            PlanStreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(Expression.class),
            testCase.getConfiguration() // The configuration query should be == to the source text of the function for this to work
        );
        // Fields use synthetic sources, which can't be serialized. So we use the originals instead.
        return newExpression.replaceChildren(expression.children());
    }
}
