/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for testing full-text functions that don't operate on a field, such as KQL.
 * It provides utilities for building test cases with string parameters.
 */
public abstract class NoFieldFullTextFunctionTestCase extends SingleFieldFullTextFunctionTestCase {

    public NoFieldFullTextFunctionTestCase(Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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

}
