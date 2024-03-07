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
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;
import org.elasticsearch.xpack.qlcore.type.DataTypes;

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
        booleans(cases, "mv_first", "MvFirst", DataTypes.BOOLEAN, (size, values) -> equalTo(values.findFirst().get()));
        bytesRefs(cases, "mv_first", "MvFirst", Function.identity(), (size, values) -> equalTo(values.findFirst().get()));
        doubles(cases, "mv_first", "MvFirst", DataTypes.DOUBLE, (size, values) -> equalTo(values.findFirst().getAsDouble()));
        ints(cases, "mv_first", "MvFirst", DataTypes.INTEGER, (size, values) -> equalTo(values.findFirst().getAsInt()));
        longs(cases, "mv_first", "MvFirst", DataTypes.LONG, (size, values) -> equalTo(values.findFirst().getAsLong()));
        unsignedLongs(cases, "mv_first", "MvFirst", DataTypes.UNSIGNED_LONG, (size, values) -> equalTo(values.findFirst().get()));
        dateTimes(cases, "mv_first", "MvFirst", DataTypes.DATETIME, (size, values) -> equalTo(values.findFirst().getAsLong()));
        geoPoints(cases, "mv_first", "MvFirst", EsqlDataTypes.GEO_POINT, (size, values) -> equalTo(values.findFirst().get()));
        cartesianPoints(cases, "mv_first", "MvFirst", EsqlDataTypes.CARTESIAN_POINT, (size, values) -> equalTo(values.findFirst().get()));
        geoShape(cases, "mv_first", "MvFirst", EsqlDataTypes.GEO_SHAPE, (size, values) -> equalTo(values.findFirst().get()));
        cartesianShape(cases, "mv_first", "MvFirst", EsqlDataTypes.CARTESIAN_SHAPE, (size, values) -> equalTo(values.findFirst().get()));
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(false, cases)));
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvFirst(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representableTypes();
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }
}
