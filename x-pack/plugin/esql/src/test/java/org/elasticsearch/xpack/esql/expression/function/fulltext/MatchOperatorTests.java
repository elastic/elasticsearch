/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

/**
 * This class is only used to generates docs for the match operator - all testing is done in {@link MatchTests}
 */
@FunctionName("match_operator")
public class MatchOperatorTests extends MatchTests {

    public MatchOperatorTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // Have a minimal test so that we can generate the appropriate types in the docs
        List<TestCaseSupplier> suppliers = new LinkedList<>();
        addPositiveTestCase(List.of(DataType.KEYWORD, DataType.KEYWORD), suppliers);
        addPositiveTestCase(List.of(DataType.TEXT, DataType.TEXT), suppliers);
        addPositiveTestCase(List.of(DataType.KEYWORD, DataType.TEXT), suppliers);
        addPositiveTestCase(List.of(DataType.TEXT, DataType.KEYWORD), suppliers);
        return parameterSuppliersFromTypedData(suppliers);
    }
}
