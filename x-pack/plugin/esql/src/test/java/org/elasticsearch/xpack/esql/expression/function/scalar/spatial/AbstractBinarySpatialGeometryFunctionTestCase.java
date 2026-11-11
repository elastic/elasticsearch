/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.GeometryDocSvg;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialBinaryGeometryBlockProcessor.flattenIfHeterogeneousCollection;

/**
 * Base test case for spatial functions that take two geometry arguments and return a geometry
 * (e.g. ST_UNION, ST_INTERSECTION, ST_DIFFERENCE, ST_SYMDIFFERENCE).
 */
public abstract class AbstractBinarySpatialGeometryFunctionTestCase extends AbstractScalarFunctionTestCase {

    protected AbstractBinarySpatialGeometryFunctionTestCase(Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /** The JTS operation to apply for computing expected results. */
    protected abstract BiFunction<Geometry, Geometry, Geometry> jtsOperation();

    /** The evaluator class name prefix, e.g. "StUnion" or "StIntersection". */
    protected abstract String evaluatorPrefix();

    /** The expected return type for the given pair of input spatial types. */
    protected DataType expectedReturnType(DataType leftType, DataType rightType) {
        if (DataType.isSpatialGeo(leftType) || DataType.isSpatialGeo(rightType)) {
            return GEO_SHAPE;
        }
        return CARTESIAN_SHAPE;
    }

    protected static Iterable<Object[]> buildParameters(String evaluatorPrefix, BiFunction<Geometry, Geometry, Geometry> jtsOp) {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        DataType[][] typePairs = {
            { GEO_POINT, GEO_POINT },
            { GEO_POINT, GEO_SHAPE },
            { GEO_SHAPE, GEO_POINT },
            { GEO_SHAPE, GEO_SHAPE },
            { CARTESIAN_POINT, CARTESIAN_POINT },
            { CARTESIAN_POINT, CARTESIAN_SHAPE },
            { CARTESIAN_SHAPE, CARTESIAN_POINT },
            { CARTESIAN_SHAPE, CARTESIAN_SHAPE } };

        String evaluatorName = evaluatorPrefix + "SourceAndSourceEvaluator[left=Attribute[channel=0], right=Attribute[channel=1]]";

        for (DataType[] pair : typePairs) {
            DataType leftType = pair[0];
            DataType rightType = pair[1];
            DataType expectedType = (DataType.isSpatialGeo(leftType) || DataType.isSpatialGeo(rightType)) ? GEO_SHAPE : CARTESIAN_SHAPE;
            TestCaseSupplier.TypedDataSupplier leftSupplier = AbstractSpatialGeometryTransformTestCase.testCaseSupplier(leftType);
            TestCaseSupplier.TypedDataSupplier rightSupplier = AbstractSpatialGeometryTransformTestCase.testCaseSupplier(rightType);
            String testName = leftType.typeName() + " and " + rightType.typeName();
            suppliers.add(new TestCaseSupplier(testName, List.of(leftType, rightType), () -> {
                TestCaseSupplier.TypedData leftData = leftSupplier.get();
                TestCaseSupplier.TypedData rightData = rightSupplier.get();
                BytesRef leftWkb = (BytesRef) leftData.data();
                BytesRef rightWkb = (BytesRef) rightData.data();
                var expectedResult = valueOf(leftWkb, rightWkb, jtsOp);
                return new TestCaseSupplier.TestCase(
                    List.of(leftData, rightData),
                    evaluatorName,
                    expectedType,
                    Matchers.equalTo(expectedResult)
                );
            }));
        }

        // Add hardcoded suppliers
        List<TestCaseSupplier.TypedDataSupplier> hardcoded = AbstractSpatialGeometryTransformTestCase.hardcodedSuppliers();
        // hardcoded order: GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE
        // pair each with its CRS-compatible partner
        int[][] hardcodedPairs = { { 0, 0 }, { 0, 2 }, { 2, 0 }, { 2, 2 }, { 1, 1 }, { 1, 3 }, { 3, 1 }, { 3, 3 } };
        DataType[] hardcodedTypes = { GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE };
        for (int[] pair : hardcodedPairs) {
            int leftIdx = pair[0];
            int rightIdx = pair[1];
            DataType leftType = hardcodedTypes[leftIdx];
            DataType rightType = hardcodedTypes[rightIdx];
            DataType expectedType = (DataType.isSpatialGeo(leftType) || DataType.isSpatialGeo(rightType)) ? GEO_SHAPE : CARTESIAN_SHAPE;
            TestCaseSupplier.TypedDataSupplier leftSupplier = hardcoded.get(leftIdx);
            TestCaseSupplier.TypedDataSupplier rightSupplier = hardcoded.get(rightIdx);
            String testName = "hardcoded " + leftType.typeName() + " and " + rightType.typeName();
            suppliers.add(new TestCaseSupplier(testName, List.of(leftType, rightType), () -> {
                TestCaseSupplier.TypedData leftData = leftSupplier.get();
                TestCaseSupplier.TypedData rightData = rightSupplier.get();
                BytesRef leftWkb = (BytesRef) leftData.data();
                BytesRef rightWkb = (BytesRef) rightData.data();
                var expectedResult = valueOf(leftWkb, rightWkb, jtsOp);
                return new TestCaseSupplier.TestCase(
                    List.of(leftData, rightData),
                    evaluatorName,
                    expectedType,
                    Matchers.equalTo(expectedResult)
                );
            }));
        }

        var testSuppliers = anyNullIsNull(
            randomizeBytesRefsOffset(suppliers),
            (nullPosition, nullValueDataType, original) -> nullValueDataType == DataType.NULL ? DataType.NULL : original.expectedType(),
            (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
        );

        return parameterSuppliersFromTypedData(testSuppliers);
    }

    private static BytesRef valueOf(BytesRef leftWkb, BytesRef rightWkb, BiFunction<Geometry, Geometry, Geometry> jtsOp) {
        if (leftWkb == null || rightWkb == null) {
            return null;
        }
        try {
            Geometry leftGeom = flattenIfHeterogeneousCollection(UNSPECIFIED.wkbToJtsGeometry(leftWkb));
            Geometry rightGeom = flattenIfHeterogeneousCollection(UNSPECIFIED.wkbToJtsGeometry(rightWkb));
            Geometry result = jtsOp.apply(leftGeom, rightGeom);
            return UNSPECIFIED.jtsGeometryToWkb(result);
        } catch (Exception e) {
            throw new AssumptionViolatedException("Skipping invalid test case");
        }
    }

    /**
     * Creates a geometry diagram showing two input geometries (as outlines) and their
     * combined result (filled). Useful for documenting binary spatial operations.
     */
    protected static DocsV3Support.GeometryDiagram binaryDiagram(
        String name,
        String title,
        String description,
        String wktA,
        String wktB,
        GeometryDocSvg.Config config,
        BiFunction<Geometry, Geometry, Geometry> jtsOp
    ) {
        Geometry jtsA = jts(wktA);
        Geometry jtsB = jts(wktB);
        Geometry jtsResult = jtsOp.apply(jtsA, jtsB);
        return new DocsV3Support.GeometryDiagram(
            name,
            title,
            description,
            config,
            List.of(
                GeometryDocSvg.Layer.outline(toEs(jtsA)),
                GeometryDocSvg.Layer.outline(toEs(jtsB)),
                GeometryDocSvg.Layer.filled(toEs(jtsResult))
            )
        );
    }

    /** Parse a WKT string into a JTS geometry. */
    protected static Geometry jts(String wkt) {
        try {
            return new org.locationtech.jts.io.WKTReader().read(wkt);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new AssertionError("invalid wkt: " + wkt, e);
        }
    }

    /** Convert a JTS geometry to an Elasticsearch geometry by round-tripping through WKT. */
    protected static org.elasticsearch.geometry.Geometry toEs(Geometry jtsGeom) {
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
