/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.stringCases;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractFullTextFunctionTestCase extends AbstractFunctionTestCase {

    /**
     * Adds test cases with null field (first argument) to the provided list of suppliers.
     * This creates copies of existing test cases but with the field parameter set to null,
     * which tests how full-text functions handle missing/null fields.
     *
     * @param suppliers the list of test case suppliers to augment
     * @return the same list with additional null-field test cases added
     */
    protected static List<TestCaseSupplier> addNullFieldTestCases(List<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> nullFieldCases = new ArrayList<>();

        Set<List<DataType>> uniqueSignatures = new HashSet<>();
        for (TestCaseSupplier supplier : suppliers) {
            boolean firstTimeSeenSignature = uniqueSignatures.add(supplier.types());
            // Add a single null field case per unique signature, similar to AbstractFunctionTestCase.anyNullIsNull
            if (firstTimeSeenSignature == false) {
                continue;
            }
            // Create a new test case supplier with null as the first argument (field)
            List<DataType> types = new ArrayList<>(supplier.types());
            types.set(0, DataType.NULL);
            TestCaseSupplier nullFieldCase = new TestCaseSupplier(supplier.name() + " with null field", types, () -> {
                TestCaseSupplier.TestCase original = supplier.supplier().get();
                List<TestCaseSupplier.TypedData> modifiedData = new ArrayList<>(original.getData());

                // Replace the first argument (field) with null
                TestCaseSupplier.TypedData originalField = modifiedData.get(0);
                modifiedData.set(0, new TestCaseSupplier.TypedData(null, DataType.NULL, originalField.name()));

                // Return a test case that expects null result since field is null
                return new TestCaseSupplier.TestCase(modifiedData, original.evaluatorToString(), DataType.BOOLEAN, equalTo(null));
            });
            nullFieldCases.add(nullFieldCase);
        }

        suppliers.addAll(nullFieldCases);
        return suppliers;
    }

    protected static List<TestCaseSupplier> addStringTestCases(List<TestCaseSupplier> suppliers) {
        for (DataType fieldType : DataType.stringTypes()) {
            if (DataType.UNDER_CONSTRUCTION.contains(fieldType)) {
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
            }
        }
        return suppliers;
    }

    /**
     * Adds function named parameters to all the test case suppliers provided
     */
    protected static List<TestCaseSupplier> addFunctionNamedParams(
        List<TestCaseSupplier> suppliers,
        Supplier<MapExpression> mapExpressionSupplier
    ) {
        List<TestCaseSupplier> result = new ArrayList<>();
        for (TestCaseSupplier supplier : suppliers) {
            result.add(supplier);
            List<DataType> dataTypes = new ArrayList<>(supplier.types());
            dataTypes.add(UNSUPPORTED);
            result.add(new TestCaseSupplier(supplier.name() + ", options", dataTypes, () -> {
                List<TestCaseSupplier.TypedData> values = new ArrayList<>(supplier.get().getData());
                values.add(
                    new TestCaseSupplier.TypedData(
                        mapExpressionSupplier.get(),
                        UNSUPPORTED,
                        "options"
                    ).forceLiteral()
                );

                return new TestCaseSupplier.TestCase(values, equalTo("MatchEvaluator"), BOOLEAN, equalTo(true));
            }));
        }
        return result;
    }

    /**
     * Copy of the overridden method that doesn't check for children size, as the {@code options} child aren't serialized in
     * full text functions, but passed to the QueryBuilder instead
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

    /**
     * Checks that a literal full text functions built from the test case is resolved, which means that any type resolution logic
     * has been applied
     */
    public final void testLiteralExpressions() {
        Expression expression = buildLiteralExpression(testCase);
        assertFalse("expected resolved", expression.typeResolved().unresolved());
    }
}
