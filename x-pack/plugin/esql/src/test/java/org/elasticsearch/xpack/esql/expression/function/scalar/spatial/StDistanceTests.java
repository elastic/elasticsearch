/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

@FunctionName("st_distance")
public class StDistanceTests extends BinarySpatialFunctionTestCase {
    public StDistanceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        DataType[] geoDataTypes = { DataType.GEO_POINT };
        StDistanceTests.addSpatialCombinations(suppliers, geoDataTypes);
        DataType[] cartesianDataTypes = { DataType.CARTESIAN_POINT };
        StDistanceTests.addSpatialCombinations(suppliers, cartesianDataTypes);
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), StDistanceTests::typeErrorMessage)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StDistance(source, args.get(0), args.get(1));
    }

    protected static void addSpatialCombinations(List<TestCaseSupplier> suppliers, DataType[] dataTypes) {
        addSpatialCombinations(suppliers, dataTypes, DataType.DOUBLE, true);
    }

    protected static String typeErrorMessage(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        return typeErrorMessage(includeOrdinal, validPerPosition, types, true);
    }
}
