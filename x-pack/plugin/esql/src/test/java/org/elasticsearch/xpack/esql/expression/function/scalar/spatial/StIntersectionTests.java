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
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.GeometryDocSvg;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@FunctionName("st_intersection")
public class StIntersectionTests extends AbstractBinarySpatialGeometryFunctionTestCase {
    public StIntersectionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return buildParameters("StIntersection", (left, right) -> left.intersection(right));
    }

    @Override
    protected BiFunction<Geometry, Geometry, Geometry> jtsOperation() {
        return (left, right) -> left.intersection(right);
    }

    @Override
    protected String evaluatorPrefix() {
        return "StIntersection";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StIntersection(source, args.get(0), args.get(1));
    }

    /**
     * Diagrams shown in the generated docs to illustrate the intersection of two overlapping polygons.
     * The diagram shows both input polygons as outlines with the intersection result filled on top.
     */
    public static List<DocsV3Support.GeometryDiagram> geometryDiagrams() {
        String wktA = "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))";
        String wktB = "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))";
        GeometryDocSvg.Config config = GeometryDocSvg.Config.DEFAULT.width(360).height(360);
        return List.of(
            binaryDiagram(
                "intersection",
                "Intersection of two overlapping polygons",
                "The intersection is the shared area of both polygons.",
                wktA,
                wktB,
                config,
                (a, b) -> a.intersection(b)
            )
        );
    }
}
