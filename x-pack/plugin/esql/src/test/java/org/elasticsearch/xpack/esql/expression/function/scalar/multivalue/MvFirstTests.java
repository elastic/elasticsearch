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

public class MvFirstTests extends AbstractMultivalueFunctionTestCase {
    public MvFirstTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_first", "MvFirst", DataType.BOOLEAN, (size, values) -> equalTo(values.findFirst().get()));
        bytesRefs(cases, "mv_first", "MvFirst", Function.identity(), (size, values) -> equalTo(values.findFirst().get()));
        doubles(cases, "mv_first", "MvFirst", DataType.DOUBLE, (size, values) -> equalTo(values.findFirst().getAsDouble()));
        ints(cases, "mv_first", "MvFirst", DataType.INTEGER, (size, values) -> equalTo(values.findFirst().getAsInt()));
        longs(cases, "mv_first", "MvFirst", DataType.LONG, (size, values) -> equalTo(values.findFirst().getAsLong()));
        unsignedLongs(cases, "mv_first", "MvFirst", DataType.UNSIGNED_LONG, (size, values) -> equalTo(values.findFirst().get()));
        dateTimes(cases, "mv_first", "MvFirst", DataType.DATETIME, (size, values) -> equalTo(values.findFirst().getAsLong()));
        dateNanos(cases, "mv_first", "MvFirst", DataType.DATE_NANOS, (size, values) -> equalTo(values.findFirst().getAsLong()));
        geoPoints(cases, "mv_first", "MvFirst", DataType.GEO_POINT, (size, values) -> equalTo(values.findFirst().get()));
        cartesianPoints(cases, "mv_first", "MvFirst", DataType.CARTESIAN_POINT, (size, values) -> equalTo(values.findFirst().get()));
        geoShape(cases, "mv_first", "MvFirst", DataType.GEO_SHAPE, (size, values) -> equalTo(values.findFirst().get()));
        cartesianShape(cases, "mv_first", "MvFirst", DataType.CARTESIAN_SHAPE, (size, values) -> equalTo(values.findFirst().get()));
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, cases);
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvFirst(source, field);
    }
}
