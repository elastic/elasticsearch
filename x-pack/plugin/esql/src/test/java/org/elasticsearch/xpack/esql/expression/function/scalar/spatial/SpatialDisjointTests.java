/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@FunctionName("st_disjoint")
public class SpatialDisjointTests extends SpatialRelatesFunctionTestCase {
    public SpatialDisjointTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        DataType[] geoDataTypes = { EsqlDataTypes.GEO_POINT, EsqlDataTypes.GEO_SHAPE };
        SpatialRelatesFunctionTestCase.addSpatialCombinations(suppliers, geoDataTypes);
        DataType[] cartesianDataTypes = { EsqlDataTypes.CARTESIAN_POINT, EsqlDataTypes.CARTESIAN_SHAPE };
        SpatialRelatesFunctionTestCase.addSpatialCombinations(suppliers, cartesianDataTypes);
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), SpatialDisjointTests::typeErrorMessage)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialDisjoint(source, args.get(0), args.get(1));
    }
}
