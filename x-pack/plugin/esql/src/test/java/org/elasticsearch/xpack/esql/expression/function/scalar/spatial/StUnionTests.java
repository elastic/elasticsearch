/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.GeometryDocSvg;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;

@FunctionName("st_union")
public class StUnionTests extends AbstractBinarySpatialGeometryFunctionTestCase {
    public StUnionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // Binary cases (ST_UNION(geomA, geomB))
        List<Object[]> all = new ArrayList<>();
        for (Object[] o : buildParameters("StUnion", (left, right) -> left.union(right))) {
            all.add(o);
        }
        // Unary cases (ST_UNION(geom)) — single value returned unchanged
        for (Object[] o : buildUnaryParameters()) {
            all.add(o);
        }
        return all;
    }

    /**
     * Builds test cases for the unary form {@code ST_UNION(geom)}, which returns a single geometry
     * value unchanged. Multi-value cases are covered by csv-spec integration tests.
     */
    private static final FunctionAppliesTo UNARY_APPLIES_TO = TestCaseSupplier.appliesTo(
        FunctionAppliesToLifecycle.PREVIEW,
        "9.6.0",
        "",
        false
    );

    static Iterable<Object[]> buildUnaryParameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        String evaluatorName = "StUnionUnarySourceEvaluator[geom=Attribute[channel=0]]";
        for (DataType type : new DataType[] { GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE }) {
            DataType expectedType = DataType.isSpatialGeo(type) ? GEO_SHAPE : CARTESIAN_SHAPE;
            TestCaseSupplier.TypedDataSupplier supplier = AbstractSpatialGeometryTransformTestCase.testCaseSupplier(type);
            suppliers.add(new TestCaseSupplier(type.typeName(), List.of(type), () -> {
                TestCaseSupplier.TypedData data = supplier.get().withAppliesTo(UNARY_APPLIES_TO);
                BytesRef wkb = (BytesRef) data.data();
                return new TestCaseSupplier.TestCase(List.of(data), evaluatorName, expectedType, Matchers.equalTo(wkb));
            }));
        }
        // Hardcoded cases: GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE
        List<TestCaseSupplier.TypedDataSupplier> hardcoded = AbstractSpatialGeometryTransformTestCase.hardcodedSuppliers();
        DataType[] hardcodedTypes = { GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE };
        for (int i = 0; i < hardcodedTypes.length; i++) {
            DataType type = hardcodedTypes[i];
            DataType expectedType = DataType.isSpatialGeo(type) ? GEO_SHAPE : CARTESIAN_SHAPE;
            TestCaseSupplier.TypedDataSupplier hSupplier = hardcoded.get(i);
            suppliers.add(new TestCaseSupplier("hardcoded " + type.typeName(), List.of(type), () -> {
                TestCaseSupplier.TypedData data = hSupplier.get().withAppliesTo(UNARY_APPLIES_TO);
                BytesRef wkb = (BytesRef) data.data();
                return new TestCaseSupplier.TestCase(List.of(data), evaluatorName, expectedType, Matchers.equalTo(wkb));
            }));
        }
        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                randomizeBytesRefsOffset(suppliers),
                (nullPosition, nullValueDataType, original) -> nullValueDataType == DataType.NULL ? DataType.NULL : original.expectedType(),
                (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
            )
        );
    }

    @Override
    protected BiFunction<Geometry, Geometry, Geometry> jtsOperation() {
        return (left, right) -> left.union(right);
    }

    @Override
    protected String evaluatorPrefix() {
        return "StUnion";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return args.size() == 1 ? new StUnionUnary(source, args.get(0)) : new StUnion(source, args.get(0), args.get(1));
    }

    /**
     * Diagrams shown in the generated docs to illustrate the union of two overlapping polygons.
     * The diagram shows both input polygons as outlines with the union result filled on top.
     */
    public static List<DocsV3Support.GeometryDiagram> geometryDiagrams() {
        String wktA = "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))";
        String wktB = "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))";
        GeometryDocSvg.Config config = GeometryDocSvg.Config.DEFAULT.width(360).height(360);
        return List.of(
            binaryDiagram(
                "union",
                "Union of two overlapping polygons",
                "The union of two overlapping squares covers the area of both.",
                wktA,
                wktB,
                config,
                (a, b) -> a.union(b)
            )
        );
    }
}
