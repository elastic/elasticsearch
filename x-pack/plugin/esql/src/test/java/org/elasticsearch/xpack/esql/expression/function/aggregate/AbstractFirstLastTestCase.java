/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.unlimitedSuppliers;
import static org.hamcrest.Matchers.anyOf;

public abstract class AbstractFirstLastTestCase extends AbstractAggregationTestCase {

    public static Iterable<Object[]> parameters(boolean isFirst, boolean isNullable) {
        int rows = 1000;
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        List<DataType> types = new ArrayList<>( List.of(INTEGER, LONG, DOUBLE, KEYWORD, TEXT, BOOLEAN, IP, DATETIME, DATE_NANOS));

        if (isNullable) {
            types.add(DataType.BOOLEAN);
        }

        for (DataType valueType : types) {
            for (TestCaseSupplier.TypedDataSupplier valueSupplier : unlimitedSuppliers(valueType, rows, rows)) {
                for (DataType sortType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
                    for (TestCaseSupplier.TypedDataSupplier sortSupplier : unlimitedSuppliers(sortType, rows, rows)) {
                        suppliers.add(makeSupplier(valueSupplier, sortSupplier, isFirst, isNullable));
                    }
                }
            }
        }

        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier valueSupplier,
        TestCaseSupplier.TypedDataSupplier sortSupplier,
        boolean first,
        boolean isNullable
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
                    (isNullable ? "All" : "")
                        + standardAggregatorNameAllBytesTheSame(first ? "First" : "Last", values.type())
                        + "ByTimestamp",
                    values.type(),
                    anyOf(() -> Iterators.map(expected.iterator(), Matchers::equalTo))
                );
            }
        );
    }
}
