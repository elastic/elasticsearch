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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.unlimitedSuppliers;
import static org.elasticsearch.xpack.esql.expression.function.aggregate.FirstTests.makeSupplier;

public class LastTests extends AbstractAggregationTestCase {
    public LastTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        int rows = 1000;
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType valueType : List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.KEYWORD, DataType.TEXT)) {
            for (TestCaseSupplier.TypedDataSupplier valueSupplier : unlimitedSuppliers(valueType, rows, rows)) {
                for (DataType sortType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
                    for (TestCaseSupplier.TypedDataSupplier sortSupplier : unlimitedSuppliers(sortType, rows, rows)) {
                        suppliers.add(makeSupplier(valueSupplier, sortSupplier, false));
                    }
                }
            }
        }
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Last(source, args.get(0), args.get(1));
    }
}
