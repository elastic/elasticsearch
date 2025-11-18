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

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;

@FunctionName("st_disjoint")
public class SpatialDisjointTests extends SpatialRelatesFunctionTestCase {
    public SpatialDisjointTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        SpatialRelatesFunctionTestCase.addSpatialGridCombinations(suppliers, GEO_POINT);
        DataType[] geoDataTypes = { DataType.GEO_POINT, DataType.GEO_SHAPE };
        SpatialRelatesFunctionTestCase.addSpatialCombinations(suppliers, geoDataTypes);
        DataType[] cartesianDataTypes = { DataType.CARTESIAN_POINT, DataType.CARTESIAN_SHAPE };
        SpatialRelatesFunctionTestCase.addSpatialCombinations(suppliers, cartesianDataTypes);
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), SpatialDisjointTests::typeErrorMessage)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialDisjoint(source, args.get(0), args.get(1));
    }

    protected static String typeErrorMessage(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        return typeErrorMessage(includeOrdinal, validPerPosition, types, false, true);
    }
}
