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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.GeometryDocSvg;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;

@FunctionName("st_buffer")
public class StBufferTests extends AbstractSpatialGeometryTransformTestCase {
    public StBufferTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return buildParameters(
            "StBuffer",
            "distance",
            StBufferTests::bufferOp,
            (spatial, param) -> DataType.isSpatialGeo(spatial) ? GEO_SHAPE : CARTESIAN_SHAPE,
            1,
            10
        );
    }

    private static Geometry bufferOp(Geometry geometry, double distance) {
        if (distance == 0) {
            return geometry;
        }
        return BufferOp.bufferOp(geometry, distance);
    }

    @Override
    protected BiFunction<Geometry, Double, Geometry> jtsOperation() {
        return StBufferTests::bufferOp;
    }

    @Override
    protected String evaluatorPrefix() {
        return "StBuffer";
    }

    @Override
    protected String secondParameterName() {
        return "distance";
    }

    @Override
    protected DataType expectedReturnType(DataType spatialType) {
        return DataType.isSpatialGeo(spatialType) ? GEO_SHAPE : CARTESIAN_SHAPE;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StBuffer(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    /**
     * Diagrams shown in the generated docs to illustrate the effect of {@code distance} and the
     * named buffer options. Each entry renders the original geometry beneath the result.
     */
    public static List<DocsV3Support.GeometryDiagram> geometryDiagrams() {
        return List.of(
            diagram(
                "shrink_polygon",
                "Negative distance shrinks a polygon",
                "Buffering a 10x10 square by `-2` produces a smaller square inset by 2 units on each side.",
                "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
                BufferOp.bufferOp(jts("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"), -2)
            ),
            diagram(
                "point",
                "Buffering a point produces a circle",
                "A point with positive distance becomes a polygon approximating a circle (8 segments per quadrant by default).",
                "POINT(0 0)",
                BufferOp.bufferOp(jts("POINT(0 0)"), 1)
            ),
            diagram(
                "line_round",
                "Default round end caps on a line",
                "A line buffered with default options has rounded end caps (`endcap=round`).",
                "LINESTRING(0 0, 10 0)",
                BufferOp.bufferOp(jts("LINESTRING(0 0, 10 0)"), 2)
            ),
            diagram(
                "line_flat",
                "Flat end caps on a line",
                "Setting `endcap=flat` keeps the buffer flush with the line's endpoints.",
                "LINESTRING(0 0, 10 0)",
                bufferWith(
                    jts("LINESTRING(0 0, 10 0)"),
                    2,
                    new BufferParameters(8, BufferParameters.CAP_FLAT, BufferParameters.JOIN_ROUND, 5.0)
                )
            ),
            diagram(
                "line_square",
                "Square end caps on a line",
                "`endcap=square` extends the buffer past each endpoint by the buffer distance.",
                "LINESTRING(0 0, 10 0)",
                bufferWith(
                    jts("LINESTRING(0 0, 10 0)"),
                    2,
                    new BufferParameters(8, BufferParameters.CAP_SQUARE, BufferParameters.JOIN_ROUND, 5.0)
                )
            ),
            diagram(
                "corner_mitre",
                "Mitre joins on a buffered corner",
                "`join=mitre` produces sharp corners. Combined with `endcap=flat` you get a clean rectangle for a buffered L-shape.",
                "LINESTRING(0 0, 10 0, 10 10)",
                bufferWith(
                    jts("LINESTRING(0 0, 10 0, 10 10)"),
                    2,
                    new BufferParameters(8, BufferParameters.CAP_FLAT, BufferParameters.JOIN_MITRE, 5.0)
                )
            )
        );
    }

    private static DocsV3Support.GeometryDiagram diagram(
        String name,
        String title,
        String description,
        String inputWkt,
        Geometry jtsResult
    ) {
        return new DocsV3Support.GeometryDiagram(
            name,
            title,
            description,
            GeometryDocSvg.Config.DEFAULT.width(360).height(240),
            List.of(GeometryDocSvg.Layer.filled(toEs(jtsResult)), GeometryDocSvg.Layer.outline(parseEs(inputWkt)))
        );
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

    private static Geometry bufferWith(Geometry input, double distance, BufferParameters params) {
        return BufferOp.bufferOp(input, distance, params);
    }
}
