/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * These functions provide support for extracting multi-value spatial envelope results into various result builders
 * for ST_ENVELOPE, ST_XMAX, ST_XMIN, ST_YMAX and ST_YMIN.
 */
public class SpatialEnvelopeResults<T extends Block.Builder> {

    /**
     * This version uses the geometry visitor to handle all geometry types correctly.
     */
    void fromWellKnownBinary(
        T results,
        @Position int p,
        BytesRefBlock wkbBlock,
        SpatialEnvelopeVisitor.PointVisitor pointVisitor,
        BiConsumer<T, Rectangle> rectangleResult
    ) {
        int valueCount = wkbBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendNull();
            return;
        }
        BytesRef scratch = new BytesRef();
        var visitor = new SpatialEnvelopeVisitor(pointVisitor);
        int firstValueIndex = wkbBlock.getFirstValueIndex(p);
        for (int i = 0; i < valueCount; i++) {
            BytesRef wkb = wkbBlock.getBytesRef(firstValueIndex + i, scratch);
            var geometry = UNSPECIFIED.wkbToGeometry(wkb);
            geometry.visit(visitor);
        }
        if (pointVisitor.isValid()) {
            rectangleResult.accept(results, pointVisitor.getResult());
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    /**
     * This version uses the envelope visitor to handle wrapping longitudes correctly.
     * It should be used for ST_ENVELOPE, and for ST_X/YMIN/MAX with GEO data and x/longitude values.
     */
    void fromDocValues(
        T results,
        @Position int p,
        LongBlock encodedBlock,
        SpatialEnvelopeVisitor.PointVisitor pointVisitor,
        SpatialCoordinateTypes spatialCoordinateType,
        BiConsumer<T, Rectangle> rectangleResult
    ) {
        int valueCount = encodedBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendNull();
            return;
        }
        int firstValueIndex = encodedBlock.getFirstValueIndex(p);
        for (int i = 0; i < valueCount; i++) {
            long encoded = encodedBlock.getLong(firstValueIndex + i);
            pointVisitor.visitPoint(spatialCoordinateType.decodeX(encoded), spatialCoordinateType.decodeY(encoded));
        }
        if (pointVisitor.isValid()) {
            rectangleResult.accept(results, pointVisitor.getResult());
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    /**
     * The linear version of this function does not bother with the envelope visitor, since wrapping is not an issue.
     * It should be used for ST_X/YMIN/MAX with CARTESIAN data, and for ST_YMIN/MAX with GEO data and y/latitude values.
     */
    void fromDocValuesLinear(
        DoubleBlock.Builder results,
        @Position int p,
        LongBlock encodedBlock,
        double initialValue,
        BiFunction<Double, Long, Double> reducer
    ) {
        int valueCount = encodedBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendNull();
            return;
        }
        double current = initialValue;
        int firstValueIndex = encodedBlock.getFirstValueIndex(p);
        for (int i = 0; i < valueCount; i++) {
            long encoded = encodedBlock.getLong(firstValueIndex + i);
            current = reducer.apply(current, encoded);
        }
        if (Double.isFinite(current)) {
            results.appendDouble(current);
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }
}
