/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvCountTests extends AbstractMultivalueFunctionTestCase {
    public MvCountTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_count", "MvCount", DataTypes.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        bytesRefs(cases, "mv_count", "MvCount", t -> DataTypes.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        doubles(cases, "mv_count", "MvCount", DataTypes.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        ints(cases, "mv_count", "MvCount", DataTypes.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        longs(cases, "mv_count", "MvCount", DataTypes.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        unsignedLongs(cases, "mv_count", "MvCount", DataTypes.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        dateTimes(cases, "mv_count", "MvCount", DataTypes.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, cases)));
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvCount(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representable();
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.INTEGER;
    }
}
