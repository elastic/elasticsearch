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
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@FunctionName("st_simplifypreservetopology")
public class StSimplifyPreserveTopologyTests extends AbstractSpatialGeometryTransformTestCase {
    public StSimplifyPreserveTopologyTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return buildParameters(
            "StSimplifyPreserveTopology",
            "tolerance",
            TopologyPreservingSimplifier::simplify,
            (spatial, param) -> spatial,
            0,
            100
        );
    }

    @Override
    protected BiFunction<Geometry, Double, Geometry> jtsOperation() {
        return TopologyPreservingSimplifier::simplify;
    }

    @Override
    protected String evaluatorPrefix() {
        return "StSimplifyPreserveTopology";
    }

    @Override
    protected String secondParameterName() {
        return "tolerance";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StSimplifyPreserveTopology(source, args.get(0), args.get(1));
    }
}
