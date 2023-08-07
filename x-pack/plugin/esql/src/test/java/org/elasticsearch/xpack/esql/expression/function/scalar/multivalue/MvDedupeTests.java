/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvDedupeTests extends AbstractMultivalueFunctionTestCase {
    public MvDedupeTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("mv_dedupe(<double>)", () -> {
            List<Double> mvData = randomList(1, 100, () -> randomDouble());
            return new TestCase(
                List.of(new TypedData(mvData, DataTypes.DOUBLE, "field")),
                "MvDedupe[field=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                getMatcher(mvData)
            );
        })));
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvDedupe(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representable();
    }

    @SuppressWarnings("unchecked")
    private static Matcher<Object> getMatcher(List<?> input) {
        if (input == null) {
            return nullValue();
        }
        Set<Object> values = new HashSet<>(input);
        return switch (values.size()) {
            case 0 -> nullValue();
            case 1 -> equalTo(values.iterator().next());
            default -> (Matcher<Object>) (Matcher<?>) containsInAnyOrder(values.stream().map(Matchers::equalTo).toArray(Matcher[]::new));
        };
    }

    @Override
    protected Matcher<Object> resultMatcherForInput(List<?> input, DataType dataType) {
        return getMatcher(input);
    }

}
