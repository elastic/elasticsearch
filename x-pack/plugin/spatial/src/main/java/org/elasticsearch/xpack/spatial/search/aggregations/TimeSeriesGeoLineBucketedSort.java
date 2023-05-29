/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.simplify.SimplificationErrorCalculator;
import org.elasticsearch.geometry.simplify.StreamingGeometrySimplifier;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoLineMultiValuesSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.spatial.search.aggregations.GeoLineAggregationBuilder.SORT_FIELD;

/**
 * A bigArrays sorter of both a geo_line's sort-values and points.
 * <p>
 * This class accumulates geo_points within buckets and heapifies the
 * bucket based on whether there are too many items in the bucket that
 * need to be dropped based on their sort value.
 */
class TimeSeriesGeoLineBucketedSort {
    private final GeoLineMultiValuesSource valuesSources;
    private final SortOrder sortOrder;
    private final int bucketSize;
    private int tsidOrd;
    private Simplifier simplifier;

    TimeSeriesGeoLineBucketedSort(
        SortOrder sortOrder,
        int bucketSize,
        GeoLineMultiValuesSource valuesSources,
        int tsidOrd,
        Simplifier simplifier
    ) {
        this.valuesSources = valuesSources;
        this.sortOrder = sortOrder;
        this.bucketSize = bucketSize;
        this.tsidOrd = tsidOrd;
        this.simplifier = simplifier;
    }

    int length() {
        return simplifier.length();
    }

    void reset() {
        simplifier.reset();
    }

    public Leaf forLeaf(AggregationExecutionContext aggCtx) throws IOException {
        this.tsidOrd = aggCtx.getTsidOrd();
        return new Leaf(aggCtx.getLeafReaderContext());
    }

    public int getTsidOrd() {
        return tsidOrd;
    }

    /**
     * Wrapper for points and sort fields that it also usable in the GeometrySimplifier library,
     * allowing us to track which points will survive geometry simplification during geo_line aggregations.
     */
    private static class SimplifiablePoint extends StreamingGeometrySimplifier.PointError {
        private double sortValue;
        private long encoded;

        private SimplifiablePoint(int index, double x, double y, double sortValue) {
            super(index, x, y);
            this.sortValue = sortValue;
            setEncoded(x, y);
        }

        public StreamingGeometrySimplifier.PointError reset(int index, double x, double y, double sortValue) {
            super.reset(index, x, y);
            this.sortValue = sortValue;
            setEncoded(x, y);
            return this;
        }

        private void setEncoded(double x, double y) {
            this.encoded = (((long) GeoEncodingUtils.encodeLongitude(x)) << 32) | GeoEncodingUtils.encodeLatitude(y) & 0xffffffffL;
        }
    }

    /** Controlled memory version of Line from streaming data */
    private static class LineStream implements Geometry {
        private final long[] encodedPoints;
        private final double[] sortValues;

        private LineStream(int length, StreamingGeometrySimplifier.PointError[] points) {
            this.encodedPoints = new long[length];
            this.sortValues = new double[length];
            for (int i = 0; i < length; i++) {
                SimplifiablePoint p = (SimplifiablePoint) points[i];
                encodedPoints[i] = p.encoded;
                sortValues[i] = p.sortValue;
            }
        }

        @Override
        public ShapeType type() {
            return ShapeType.LINESTRING;
        }

        @Override
        public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
            double[] x = new double[encodedPoints.length];
            double[] y = new double[encodedPoints.length];
            for (int i = 0; i < encodedPoints.length; i++) {
                x[i] = decodeLongitude(encodedPoints[i]);
                y[i] = decodeLatitude(encodedPoints[i]);
            }
            Line line = new Line(x, y);
            return visitor.visit(line);
        }

        private static double decodeLongitude(long encoded) {
            return GeoEncodingUtils.decodeLongitude((int) (encoded >>> 32));
        }

        private static double decodeLatitude(long encoded) {
            return GeoEncodingUtils.decodeLatitude((int) (encoded & 0xffffffffL));
        }

        @Override
        public boolean isEmpty() {
            return encodedPoints.length == 0;
        }

        @Override
        public String toString() {
            return WellKnownText.toWKT(this);
        }

        public int length() {
            return encodedPoints.length;
        }
    }

    private static class TestGeometrySimplifierMonitor implements StreamingGeometrySimplifier.Monitor {
        private int addedCount;
        private int removedCount;

        public void reset() {
            addedCount = 0;
            removedCount = 0;
        }

        @Override
        public void pointAdded(String status, List<SimplificationErrorCalculator.PointLike> points) {
            addedCount++;
            System.out.println("Adding point " + points.get(points.size() - 1));
        }

        @Override
        public void pointRemoved(
            String status,
            List<SimplificationErrorCalculator.PointLike> points,
            SimplificationErrorCalculator.PointLike removed,
            double error,
            SimplificationErrorCalculator.PointLike previous,
            SimplificationErrorCalculator.PointLike next
        ) {
            addedCount++;
            removedCount++;
            System.out.println("Adding point " + points.get(points.size() - 1) + " and removing point " + removed);
        }

        @Override
        public void startSimplification(String description, int maxPoints) {}

        @Override
        public void endSimplification(String description, List<SimplificationErrorCalculator.PointLike> points) {}
    }

    /**
     * Wrapping the Streaming GeometrySimplifier allowing the aggregation to extract
     * expected points and their sort fields after simplification
     */
    static class Simplifier extends StreamingGeometrySimplifier<LineStream>
        implements
            StreamingGeometrySimplifier.PointConstructor,
            StreamingGeometrySimplifier.PointResetter {
        double currentSortValue;

        Simplifier(int maxPoints) {
            super("GeoLineTSDB", maxPoints, SimplificationErrorCalculator.TRIANGLE_AREA, new TestGeometrySimplifierMonitor());
            this.pointConstructor = this;
            this.pointResetter = this;
        }

        @Override
        public void reset() {
            System.out.println("Resetting!");
            new Exception("Stack trace").printStackTrace(System.out);
            super.reset();
        }

        @Override
        public PointError newPoint(int index, double x, double y) {
            return new SimplifiablePoint(index, x, y, currentSortValue);
        }

        @Override
        public PointError resetPoint(PointError point, int index, double x, double y) {
            return ((SimplifiablePoint) point).reset(index, x, y, currentSortValue);
        }

        @Override
        public LineStream produce() {
            return new LineStream(this.length, this.points);
        }

        public int length() {
            return length;
        }
    }

    /** Build the aggregation based on saved state from the collector phase */
    InternalGeoLine buildAggregation(
        long bucket,
        String name,
        Map<String, Object> metadata,
        boolean complete,
        boolean includeSorts,
        int size,
        Function<Long, Long> circuitBreaker
    ) {
        circuitBreaker.apply((Double.SIZE + Long.SIZE) * (long) size);
        LineStream line = simplifier.produce();
        if (line.isEmpty()) {
            // TODO: Perhaps we should return an empty geo_line here?
            throw new IllegalStateException("Invalid bucket " + bucket + " for geo_line");
        }
        double[] sortVals = line.sortValues;
        long[] bucketLine = line.encodedPoints;
        if (sortOrder == SortOrder.ASC) {
            // time-series is natively sorted DESC, so we need to reverse the order
            ArrayUtils.reverseSubArray(sortVals, 0, sortVals.length);
            ArrayUtils.reverseSubArray(bucketLine, 0, bucketLine.length);
        }
        return new InternalGeoLine(name, bucketLine, sortVals, metadata, complete, includeSorts, sortOrder, size);
    }

    protected class Leaf {
        private final SortedNumericDoubleValues docSortValues;
        private final MultiGeoPointValues docGeoPointValues;
        private final int leafReaderOrd;

        protected Leaf(LeafReaderContext ctx) throws IOException {
            System.out.println("\n\n**** Constructing new Leaf " + ctx.ord);
            this.leafReaderOrd = ctx.ord;
            // TODO: Should the sort field be hard-coded for time-series? Or just validated earlier on?
            docSortValues = valuesSources.getNumericField(SORT_FIELD.getPreferredName(), ctx);
            docGeoPointValues = valuesSources.getGeoPointField(GeoLineAggregationBuilder.POINT_FIELD.getPreferredName(), ctx);
        }

        private boolean loadSortField(int doc) throws IOException {
            System.out.println("Collecting for ctx.ord " + leafReaderOrd);
            simplifier.currentSortValue = Long.MIN_VALUE;
            if (docSortValues.advanceExact(doc)) {
                // If we get here from TSDB `position` metric, this assertion should have been made during indexing
                if (docSortValues.docValueCount() > 1) {
                    throw new AggregationExecutionException(
                        "Encountered more than one sort value for a "
                            + "single document. Use a script to combine multiple sort-values-per-doc into a single value."
                    );
                }
                assert docSortValues.docValueCount() == 1;
                simplifier.currentSortValue = docSortValues.nextValue();
                return true;
            }
            return false;
        }

        private void loadPointField(int doc) throws IOException {
            if (false == docGeoPointValues.advanceExact(doc)) {
                return;
            }

            if (docGeoPointValues.docValueCount() > 1) {
                throw new AggregationExecutionException(
                    "Encountered more than one geo_point value for a "
                        + "single document. Use a script to combine multiple geo_point-values-per-doc into a single value."
                );
            }

            final GeoPoint point = docGeoPointValues.nextValue();
            simplifier.consume(point.getX(), point.getY());
        }

        public void collect(int doc, long bucket) throws IOException {
            System.out.println("collect(" + doc + ", " + bucket + ") for sort-order " + sortOrder);
            if (loadSortField(doc)) {
                loadPointField(doc);
            }
        }
    }
}
