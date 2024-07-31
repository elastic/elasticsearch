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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvLastTests extends AbstractMultivalueFunctionTestCase {
    public MvLastTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_last", "MvLast", DataType.BOOLEAN, (size, values) -> equalTo(values.reduce((f, s) -> s).get()));
        bytesRefs(cases, "mv_last", "MvLast", Function.identity(), (size, values) -> equalTo(values.reduce((f, s) -> s).get()));
        doubles(cases, "mv_last", "MvLast", DataType.DOUBLE, (size, values) -> equalTo(values.reduce((f, s) -> s).getAsDouble()));
        ints(cases, "mv_last", "MvLast", DataType.INTEGER, (size, values) -> equalTo(values.reduce((f, s) -> s).getAsInt()));
        longs(cases, "mv_last", "MvLast", DataType.LONG, (size, values) -> equalTo(values.reduce((f, s) -> s).getAsLong()));
        unsignedLongs(cases, "mv_last", "MvLast", DataType.UNSIGNED_LONG, (size, values) -> equalTo(values.reduce((f, s) -> s).get()));
        dateTimes(cases, "mv_last", "MvLast", DataType.DATETIME, (size, values) -> equalTo(values.reduce((f, s) -> s).getAsLong()));
        geoPoints(cases, "mv_last", "MvLast", DataType.GEO_POINT, (size, values) -> equalTo(values.reduce((f, s) -> s).get()));
        cartesianPoints(cases, "mv_last", "MvLast", DataType.CARTESIAN_POINT, (size, values) -> equalTo(values.reduce((f, s) -> s).get()));
        geoShape(cases, "mv_last", "MvLast", DataType.GEO_SHAPE, (size, values) -> equalTo(values.reduce((f, s) -> s).get()));
        cartesianShape(cases, "mv_last", "MvLast", DataType.CARTESIAN_SHAPE, (size, values) -> equalTo(values.reduce((f, s) -> s).get()));
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, cases, (v, p) -> "numeric");
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvLast(source, field);
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }
}
