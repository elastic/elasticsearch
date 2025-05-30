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
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.stringCases;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("match_phrase")
public class MatchPhraseTests extends AbstractFunctionTestCase {

    public MatchPhraseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addFunctionNamedParams(testCaseSuppliers()));
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        addStringTestCases(suppliers);
        return suppliers;
    }

    public static void addStringTestCases(List<TestCaseSupplier> suppliers) {
        for (DataType fieldType : DataType.stringTypes()) {
            if (DataType.UNDER_CONSTRUCTION.containsKey(fieldType)) {
                continue;
            }
            for (TestCaseSupplier.TypedDataSupplier queryDataSupplier : stringCases(fieldType)) {
                suppliers.add(
                    TestCaseSupplier.testCaseSupplier(
                        queryDataSupplier,
                        new TestCaseSupplier.TypedDataSupplier(fieldType.typeName(), () -> randomAlphaOfLength(10), DataType.KEYWORD),
                        (d1, d2) -> equalTo("string"),
                        DataType.BOOLEAN,
                        (o1, o2) -> true
                    )
                );

                suppliers.add(
                    TestCaseSupplier.testCaseSupplier(
                        queryDataSupplier,
                        new TestCaseSupplier.TypedDataSupplier(fieldType.typeName(), () -> randomAlphaOfLength(10), DataType.TEXT),
                        (d1, d2) -> equalTo("string"),
                        DataType.BOOLEAN,
                        (o1, o2) -> true
                    )
                );
            }
        }
    }

    /**
     * Adds function named parameters to all the test case suppliers provided
     */
    private static List<TestCaseSupplier> addFunctionNamedParams(List<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> result = new ArrayList<>();
        for (TestCaseSupplier supplier : suppliers) {
            List<DataType> dataTypes = new ArrayList<>(supplier.types());
            dataTypes.add(UNSUPPORTED);
            result.add(new TestCaseSupplier(supplier.name() + ", options", dataTypes, () -> {
                List<TestCaseSupplier.TypedData> values = new ArrayList<>(supplier.get().getData());
                values.add(
                    new TestCaseSupplier.TypedData(
                        new MapExpression(
                            Source.EMPTY,
                            List.of(new Literal(Source.EMPTY, "slop", INTEGER), new Literal(Source.EMPTY, randomAlphaOfLength(10), KEYWORD))
                        ),
                        UNSUPPORTED,
                        "options"
                    ).forceLiteral()
                );

                return new TestCaseSupplier.TestCase(values, equalTo("MatchPhraseEvaluator"), BOOLEAN, equalTo(true));
            }));
        }
        return result;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        MatchPhrase matchPhrase = new MatchPhrase(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
        // We need to add the QueryBuilder to the match_phrase expression, as it is used to implement equals() and hashCode() and
        // thus test the serialization methods. But we can only do this if the parameters make sense .
        if (args.get(0) instanceof FieldAttribute && args.get(1).foldable()) {
            QueryBuilder queryBuilder = TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, matchPhrase).toQueryBuilder();
            matchPhrase.replaceQueryBuilder(queryBuilder);
        }
        return matchPhrase;
    }

    /**
     * Copy of the overridden method that doesn't check for children size, as the {@code options} child isn't serialized in MatchPhrase.
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
