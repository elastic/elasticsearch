/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class HourTests extends AbstractConfigurationFunctionTestCase {
    public HourTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new Hour(source, args.get(0), configuration);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(DatePartFunctionTestCases.cases("HourOfDay", "2023-11-04T16:13:12Z", "Z", 16));
        suppliers.addAll(DatePartFunctionTestCases.cases("HourOfDay", "2020-02-28T23:00:00Z", "+07:00", 6));
        suppliers.add(DatePartFunctionTestCases.nullCase("HourOfDay"));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }
}
