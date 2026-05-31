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
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.unlimitedSuppliers;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.anyOf;

public abstract class AbstractFirstLastTestCase extends AbstractAggregationTestCase {

    public static Iterable<Object[]> parameters(boolean isFirst) {
        int rows = 1000;
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        List<DataType> searchFieldTypes = new ArrayList<>(
            List.of(
                DataType.INTEGER,
                DataType.LONG,
                DataType.DOUBLE,
                DataType.KEYWORD,
                DataType.TEXT,
                DataType.IP,
                DataType.BOOLEAN,
                DataType.DATETIME,
                DataType.DATE_NANOS
            )
        );

        Set<DataType> taggedTypes = new HashSet<>();
        if (isFirst) {
            List<DataType> extra = List.of(
                DataType.VERSION,
                DataType.DENSE_VECTOR,
                DataType.EXPONENTIAL_HISTOGRAM,
                DataType.CARTESIAN_POINT,
                DataType.CARTESIAN_SHAPE,
                DataType.GEO_POINT,
                DataType.GEO_SHAPE,
                DataType.GEOHASH,
                DataType.GEOTILE,
                DataType.GEOHEX,
                DataType.UNSIGNED_LONG,
                DataType.TDIGEST
            );
            searchFieldTypes.addAll(extra);
            taggedTypes.addAll(extra);
        }

        FunctionAppliesTo newIn95 = appliesTo(FunctionAppliesToLifecycle.GA, "9.5.0", "", true);
        List<DataType> sortFieldTypes = List.of(DataType.INTEGER, DataType.LONG, DataType.DATETIME, DataType.DATE_NANOS, DataType.NULL);

        for (DataType searchFieldType : searchFieldTypes) {
            for (TestCaseSupplier.TypedDataSupplier valueSupplier : unlimitedSuppliers(searchFieldType, rows, rows)) {
                var taggedValueSupplier = taggedTypes.contains(searchFieldType) ? valueSupplier.withAppliesTo(newIn95) : valueSupplier;
                for (DataType sortFieldType : sortFieldTypes) {
                    var sortSuppliers = sortFieldType == DataType.NULL
                        ? MultiRowTestCaseSupplier.nullCases(rows, rows)
                        : unlimitedSuppliers(sortFieldType, rows, rows);
                    for (TestCaseSupplier.TypedDataSupplier sortSupplier : sortSuppliers) {
                        suppliers.add(makeSupplier(taggedValueSupplier, sortSupplier, isFirst));
                    }
                }
            }
        }

        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier valueSupplier,
        TestCaseSupplier.TypedDataSupplier sortSupplier,
        boolean first
    ) {
        return new TestCaseSupplier(
            valueSupplier.name() + ", " + sortSupplier.name(),
            List.of(valueSupplier.type(), sortSupplier.type()),
            () -> {
                String evaluatorStr;
                Set<Object> expected = new HashSet<>();
                TestCaseSupplier.TypedData values = valueSupplier.get();
                TestCaseSupplier.TypedData sorts = sortSupplier.get();
                List<?> valuesList = (List<?>) values.originalData();

                // DENSE_VECTOR reuses the Float suppliers, so the evaluator class name is Float-based
                DataType effectiveValueType = values.type() == DataType.DENSE_VECTOR ? DataType.FLOAT : values.type();

                if (sorts.type() == DataType.NULL) {
                    evaluatorStr = standardAggregatorNameAllBytesTheSame("Any", effectiveValueType);
                    expected.addAll(valuesList);
                } else {
                    Long firstSort = null;
                    List<?> sortsList = (List<?>) sorts.data();
                    for (int p = 0; p < valuesList.size(); p++) {
                        Long s = ((Number) sortsList.get(p)).longValue();
                        if (firstSort == null || (first ? s < firstSort : s > firstSort)) {
                            firstSort = s;
                            expected.clear();
                            expected.add(valuesList.get(p));
                        } else if (firstSort.equals(s)) {
                            expected.add(valuesList.get(p));
                        }
                    }
                    evaluatorStr = String.format(
                        Locale.ROOT,
                        "All%sBy%s",
                        standardAggregatorNameAllBytesTheSame(first ? "First" : "Last", effectiveValueType),
                        standardAggregatorNameAllBytesTheSame("", sorts.type())
                    );
                }

                return new TestCaseSupplier.TestCase(
                    List.of(values, sorts),
                    evaluatorStr,
                    values.type(),
                    anyOf(() -> Iterators.map(expected.iterator(), Matchers::equalTo))
                );
            }
        );
    }
}
