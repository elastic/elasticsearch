/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.unlimitedSuppliers;
import static org.hamcrest.Matchers.anyOf;

public class FirstTests extends AbstractAggregationTestCase {
    public FirstTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        int rows = 1000;
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType valueType : List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE)) {
            for (TestCaseSupplier.TypedDataSupplier valueSupplier : unlimitedSuppliers(valueType, rows, rows)) {
                for (DataType sortType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
                    for (TestCaseSupplier.TypedDataSupplier sortSupplier : unlimitedSuppliers(sortType, rows, rows)) {
                        suppliers.add(makeSupplier(valueSupplier, sortSupplier, true));
                    }
                }
            }
        }
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new First(source, args.get(0), args.get(1));
    }

    static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier valueSupplier,
        TestCaseSupplier.TypedDataSupplier sortSupplier,
        boolean first
    ) {
        return new TestCaseSupplier(
            valueSupplier.name() + ", " + sortSupplier.name(),
            List.of(valueSupplier.type(), sortSupplier.type()),
            () -> {
                Long firstSort = null;
                Set<Object> expected = new HashSet<>();
                TestCaseSupplier.TypedData values = valueSupplier.get();
                TestCaseSupplier.TypedData sorts = sortSupplier.get();
                List<?> valuesList = (List<?>) values.data();
                List<?> sortsList = (List<?>) sorts.data();
                for (int p = 0; p < valuesList.size(); p++) {
                    Long s = (Long) sortsList.get(p);
                    if (firstSort == null || (first ? s < firstSort : s > firstSort)) {
                        firstSort = s;
                        expected.clear();
                        expected.add(valuesList.get(p));
                    } else if (firstSort.equals(s)) {
                        expected.add(valuesList.get(p));
                    }
                }
                return new TestCaseSupplier.TestCase(
                    List.of(values, sorts),
                    "unused",
                    values.type(),
                    anyOf(() -> Iterators.map(expected.iterator(), Matchers::equalTo))
                );
            }
        );
    }
}
