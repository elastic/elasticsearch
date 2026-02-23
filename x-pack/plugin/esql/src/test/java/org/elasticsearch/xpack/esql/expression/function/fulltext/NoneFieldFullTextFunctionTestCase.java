/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.hamcrest.Matcher;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.hamcrest.Matchers.equalTo;

public abstract class NoneFieldFullTextFunctionTestCase extends AbstractFullTextFunctionTestCase {

    public NoneFieldFullTextFunctionTestCase(Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    protected static List<TestCaseSupplier> getStringTestSupplier() {
        List<TestCaseSupplier> suppliers = new LinkedList<>();
        for (DataType strType : DataType.stringTypes()) {
            suppliers.add(
                new TestCaseSupplier(
                    "<" + strType + ">",
                    List.of(strType),
                    () -> testCase(strType, randomAlphaOfLengthBetween(1, 10), equalTo(true))
                )
            );
        }
        return suppliers;
    }

    private static TestCaseSupplier.TestCase testCase(DataType strType, String str, Matcher<Boolean> matcher) {
        return new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(new BytesRef(str), strType, "query")),
            "",
            DataType.BOOLEAN,
            matcher
        );
    }

    /**
     * Copy of the overridden method that doesn't check for children size, as the {@code options} child isn't serialized in Kql.
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
