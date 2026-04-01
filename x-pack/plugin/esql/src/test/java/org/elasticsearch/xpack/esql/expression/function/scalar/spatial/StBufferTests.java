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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;

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
        return new StBuffer(source, args.get(0), args.get(1));
    }
}
