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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * These functions provide support for extracting multi-value spatial envelope results into various result builders
 * for ST_ENVELOPE, ST_XMAX, ST_XMIN, ST_YMAX and ST_YMIN.
 */
public class SpatialEnvelopeResults<T extends Block.Builder> {
    private final SpatialCoordinateTypes spatialCoordinateType;
    private final SpatialEnvelopeVisitor.PointVisitor pointVisitor;

    /**
     * A functional interface for consuming results that need access to the spatial coordinate type.
     */
    @FunctionalInterface
    public interface TypedResultsConsumer<T> {
        void accept(T results, Rectangle rectangle, SpatialCoordinateTypes type);
    }

    public SpatialEnvelopeResults(SpatialCoordinateTypes spatialCoordinateType, SpatialEnvelopeVisitor.PointVisitor pointVisitor) {
        this.spatialCoordinateType = spatialCoordinateType;
        this.pointVisitor = pointVisitor;
    }

    /**
     * This version uses the geometry visitor to handle all geometry types correctly.
     */
    void fromWellKnownBinary(T results, @Position int p, BytesRefBlock wkbBlock, BiConsumer<T, Rectangle> rectangleResult) {
        fromWellKnownBinary(results, p, wkbBlock, (r, rect, type) -> rectangleResult.accept(r, rect));
    }

    /**
     * This version uses the geometry visitor to handle all geometry types correctly,
     * and passes the spatial coordinate type to the results consumer.
     */
    void fromWellKnownBinary(T results, @Position int p, BytesRefBlock wkbBlock, TypedResultsConsumer<T> rectangleResult) {
        pointVisitor.reset();
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
            rectangleResult.accept(results, pointVisitor.getResult(), spatialCoordinateType);
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    /**
     * This version uses the envelope visitor to handle wrapping longitudes correctly.
     * It should be used for ST_ENVELOPE, and for ST_X/YMIN/MAX with GEO data and x/longitude values.
     */
    void fromDocValues(T results, @Position int p, LongBlock encodedBlock, BiConsumer<T, Rectangle> rectangleResult) {
        pointVisitor.reset();
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

    /**
     * The factory is used in the toEvaluator method so that new instances can be created for each thread.
     * This ensures that the point visitor state is not shared between threads.
     */
    protected static class Factory<T extends Block.Builder> {
        private final SpatialCoordinateTypes spatialCoordinateType;
        private final Supplier<SpatialEnvelopeVisitor.PointVisitor> pointVisitorSupplier;

        Factory(SpatialCoordinateTypes spatialCoordinateType, Supplier<SpatialEnvelopeVisitor.PointVisitor> pointVisitorSupplier) {
            this.spatialCoordinateType = spatialCoordinateType;
            this.pointVisitorSupplier = pointVisitorSupplier;
        }

        public SpatialEnvelopeResults<T> get(DriverContext ignored) {
            return new SpatialEnvelopeResults<>(spatialCoordinateType, pointVisitorSupplier.get());
        }
    }
}
