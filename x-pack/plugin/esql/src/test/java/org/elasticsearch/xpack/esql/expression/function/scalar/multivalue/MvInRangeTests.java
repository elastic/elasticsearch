/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvInRangeTests extends AbstractScalarFunctionTestCase {
    public MvInRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // The load-bearing case: an element-wise existential, not an envelope overlap. [0,100] does NOT intersect [40,60].
        addCase(suppliers, DataType.INTEGER, "Int", List.of(0, 100), 40, 60, false);
        addCase(suppliers, DataType.INTEGER, "Int", List.of(0, 50, 100), 40, 60, true); // 50 is inside
        addCase(suppliers, DataType.INTEGER, "Int", List.of(2), 2, 3, true); // inclusive lower bound
        addCase(suppliers, DataType.INTEGER, "Int", List.of(4), 2, 3, false); // above the range
        addCase(suppliers, DataType.LONG, "Long", List.of(0L, 100L), 40L, 60L, false);
        addCase(suppliers, DataType.LONG, "Long", List.of(50L), 40L, 60L, true);
        addCase(suppliers, DataType.DOUBLE, "Double", List.of(0.0, 100.0), 40.0, 60.0, false);
        addCase(suppliers, DataType.DOUBLE, "Double", List.of(50.0), 40.0, 60.0, true);
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void addCase(
        List<TestCaseSupplier> suppliers,
        DataType type,
        String eval,
        List<?> field,
        Object lower,
        Object upper,
        boolean expected
    ) {
        suppliers.add(
            new TestCaseSupplier(
                field + " in [" + lower + "," + upper + "]",
                List.of(type, type, type),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(field, type, "field"),
                        new TestCaseSupplier.TypedData(lower, type, "lower"),
                        new TestCaseSupplier.TypedData(upper, type, "upper")
                    ),
                    "MvInRange" + eval + "Evaluator[field=Attribute[channel=0], lower=Attribute[channel=1], upper=Attribute[channel=2]]",
                    DataType.BOOLEAN,
                    equalTo(expected)
                )
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvInRange(source, args.get(0), args.get(1), args.get(2));
    }

    // mv_in_range never returns null: an empty/null field is false, so an all-null position evaluates to false.
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(false);
    }
}
