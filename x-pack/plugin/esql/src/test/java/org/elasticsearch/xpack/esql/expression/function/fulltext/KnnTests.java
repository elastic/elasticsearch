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
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.AbstractMatchFullTextFunctionTests.addNullFieldTestCases;
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
            new TestCaseSupplier(
                List.of(DENSE_VECTOR, DENSE_VECTOR),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            new FieldAttribute(
                                Source.EMPTY,
                                randomIdentifier(),
                                new EsField(randomIdentifier(), DENSE_VECTOR, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                            ),
                            DENSE_VECTOR,
                            "dense_vector field"
                        ),
                        new TestCaseSupplier.TypedData(randomDenseVector(), DENSE_VECTOR, "query")
                    ),
                    equalTo("KnnEvaluator" + KnnTests.class.getSimpleName()),
                    BOOLEAN,
                    equalTo(true)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                List.of(TEXT, DENSE_VECTOR),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            new FieldAttribute(
                                Source.EMPTY,
                                randomIdentifier(),
                                new EsField(randomIdentifier(), TEXT, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                            ),
                            TEXT,
                            "text field"
                        ),
                        new TestCaseSupplier.TypedData(randomDenseVector(), DENSE_VECTOR, "query")
                    ),
                    equalTo("KnnEvaluator" + KnnTests.class.getSimpleName()),
                    BOOLEAN,
                    equalTo(true)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                List.of(NULL, DENSE_VECTOR),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Literal.NULL, NULL, "text field"),
                        new TestCaseSupplier.TypedData(randomDenseVector(), DENSE_VECTOR, "query")
                    ),
                    equalTo("KnnEvaluator" + KnnTests.class.getSimpleName()),
                    NULL,
                    equalTo(true)
                )
            )
        );

        return addNullFieldTestCases(suppliers);
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
                        new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))),
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

    public void testSerializationOfSimple() {
        // do nothing
        assumeTrue("can't serialize function", canSerialize());
        Expression expression = buildFieldExpression(testCase);
        if (expression instanceof Knn knn) {
            // The K parameter is not serialized, so we need to remove it from the children
            // before we compare the serialization results
            List<Expression> newChildren = knn.children();
            newChildren.set(2, null); // remove the k parameter
            Expression knnWithoutK = knn.replaceChildren(newChildren);
            assertSerialization(knnWithoutK, testCase.getConfiguration());
        } else {
            // If not a Knn instance we fail the test as it is supposed to be a Knn function
            fail("Expression is not Knn");
        }
    }
}
