/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.GeometryDocSvg;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;

@FunctionName("st_simplify")
public class StSimplifyTests extends AbstractSpatialGeometryTransformTestCase {
    public StSimplifyTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return buildParameters("StSimplify", "tolerance", DouglasPeuckerSimplifier::simplify, (spatial, param) -> spatial, 0, 100);
    }

    @Override
    protected BiFunction<Geometry, Double, Geometry> jtsOperation() {
        return DouglasPeuckerSimplifier::simplify;
    }

    @Override
    protected String evaluatorPrefix() {
        return "StSimplify";
    }

    @Override
    protected String secondParameterName() {
        return "tolerance";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StSimplify(source, args.get(0), args.get(1));
    }

    /**
     * Diagrams shown in the generated docs to illustrate how tolerance affects simplification.
     * Each diagram shows the original geometry as a gray outline with the simplified result
     * filled on top.
     */
    public static List<DocsV3Support.GeometryDiagram> geometryDiagrams() {
        // A bumpy polygon: a square with small dents along the top and bottom edges.
        String bumpy = "POLYGON((0 0, 1 0.1, 2 0, 3 0.1, 4 0, 4 2, 3 1.9, 2 2, 1 1.9, 0 2, 0 0))";
        // A rough circle approximated with many vertices, taken from the PostGIS docs example.
        String circle = "POLYGON((11 3,10.91 1.69,10.66 0.41,10.24 -0.83,9.66 -2,8.93 -3.09,8.07 -4.07,7.09 -4.93,"
            + "6 -5.66,4.83 -6.24,3.59 -6.66,2.31 -6.91,1 -7,-0.31 -6.91,-1.59 -6.66,-2.83 -6.24,-4 -5.66,"
            + "-5.09 -4.93,-6.07 -4.07,-6.93 -3.09,-7.66 -2,-8.24 -0.83,-8.66 0.41,-8.91 1.69,-9 3,"
            + "-8.91 4.31,-8.66 5.59,-8.24 6.83,-7.66 8,-6.93 9.09,-6.07 10.07,-5.09 10.93,-4 11.66,"
            + "-2.83 12.24,-1.59 12.66,-0.31 12.91,1 13,2.31 12.91,3.59 12.66,4.83 12.24,6 11.66,"
            + "7.09 10.93,8.07 10.07,8.93 9.09,9.66 8,10.24 6.83,10.66 5.59,10.91 4.31,11 3))";
        GeometryDocSvg.Config bumpyConfig = GeometryDocSvg.Config.DEFAULT.width(460).height(240);
        GeometryDocSvg.Config circleConfig = GeometryDocSvg.Config.DEFAULT.width(340).height(340);
        GeometryDocSvg.Config franceConfig = GeometryDocSvg.Config.DEFAULT.width(460).height(420).aspectRatio(0.75);
        return List.of(
            diagram(
                "low_tolerance",
                "Low tolerance keeps almost every vertex",
                "With a small tolerance, only collinear or near-collinear vertices are removed.",
                bumpy,
                0.05,
                bumpyConfig
            ),
            diagram(
                "mid_tolerance",
                "Mid tolerance smooths out the bumps",
                "A tolerance of `0.2` is large enough to remove the small dents along each edge, leaving the underlying square.",
                bumpy,
                0.2,
                bumpyConfig
            ),
            diagram(
                "circle_quarter",
                "Simplifying a circle with tolerance 0.5",
                "Higher tolerances drop more vertices and the circle becomes a rough polygon.",
                circle,
                0.5,
                circleConfig
            ),
            diagram(
                "circle_octagon",
                "Simplifying a circle to an octagon",
                "Tolerance `1.0` reduces the circle to a regular octagon.",
                circle,
                1.0,
                circleConfig
            ),
            // Real-world coastline (France, MultiPolygon) at two tolerance levels. Each diagram
            // shows the simplified result on its own — both shapes visibly differ from the
            // unsimplified original and from each other.
            simplifiedDiagram(
                "france_medium",
                "Simplifying a coastline at moderate tolerance",
                "France's coastline simplified at tolerance `0.1` (roughly 10 km). Small bays and "
                    + "peninsulas survive but the coastline is noticeably smoother.",
                loadResourceWkt("France.wkt.gz"),
                0.1,
                franceConfig
            ),
            simplifiedDiagram(
                "france_coarse",
                "Simplifying a coastline at coarse tolerance",
                "France's coastline simplified at tolerance `0.5` (roughly 50 km). Only the high-level shape of the country remains.",
                loadResourceWkt("France.wkt.gz"),
                0.5,
                franceConfig
            )
        );
    }

    private static DocsV3Support.GeometryDiagram diagram(
        String name,
        String title,
        String description,
        String inputWkt,
        double tolerance,
        GeometryDocSvg.Config config
    ) {
        Geometry input = jts(inputWkt);
        Geometry simplified = DouglasPeuckerSimplifier.simplify(input, tolerance);
        return new DocsV3Support.GeometryDiagram(
            name,
            title,
            description,
            config,
            List.of(GeometryDocSvg.Layer.filled(toEs(simplified)), GeometryDocSvg.Layer.outline(parseEs(inputWkt)))
        );
    }

    /** A diagram showing only the simplified geometry — useful when the original would be too dense to read. */
    private static DocsV3Support.GeometryDiagram simplifiedDiagram(
        String name,
        String title,
        String description,
        String inputWkt,
        double tolerance,
        GeometryDocSvg.Config config
    ) {
        Geometry simplified = DouglasPeuckerSimplifier.simplify(jts(inputWkt), tolerance);
        return new DocsV3Support.GeometryDiagram(name, title, description, config, List.of(GeometryDocSvg.Layer.filled(toEs(simplified))));
    }

    private static String loadResourceWkt(String name) {
        try (
            InputStream raw = StSimplifyTests.class.getResourceAsStream(name);
            GZIPInputStream gz = new GZIPInputStream(raw);
            BufferedReader reader = new BufferedReader(new InputStreamReader(gz, StandardCharsets.UTF_8))
        ) {
            return reader.readLine();
        } catch (IOException e) {
            throw new AssertionError("failed to load test resource " + name, e);
        }
    }

    private static Geometry jts(String wkt) {
        try {
            return new org.locationtech.jts.io.WKTReader().read(wkt);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new AssertionError("invalid wkt: " + wkt, e);
        }
    }

    /** Convert a JTS geometry to an Elasticsearch geometry by round-tripping through WKT. */
    private static org.elasticsearch.geometry.Geometry toEs(Geometry jtsGeom) {
        return parseEs(jtsGeom.toText());
    }

    private static org.elasticsearch.geometry.Geometry parseEs(String wkt) {
        try {
            return WellKnownText.fromWKT(StandardValidator.instance(false), false, wkt);
        } catch (IOException | ParseException e) {
            throw new AssertionError("invalid wkt: " + wkt, e);
        }
    }
}
