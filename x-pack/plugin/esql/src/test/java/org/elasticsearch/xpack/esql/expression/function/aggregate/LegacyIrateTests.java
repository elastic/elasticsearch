/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class LegacyIrateTests extends AbstractIrateTests {
    public LegacyIrateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();
        for (List<TestCaseSupplier.TypedDataSupplier> valuesSupplier : valuesSuppliers()) {
            for (TestCaseSupplier.TypedDataSupplier fieldSupplier : valuesSupplier) {
                for (RateTests.TemporalityParameter temporality : RateTests.TemporalityParameter.values()) {
                    suppliers.add(makeSupplier(fieldSupplier, temporality, false, "LegacyIrate"));
                }
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new LegacyIrate(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW, args.get(1), args.get(2));
    }

}
