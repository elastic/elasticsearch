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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoLineMultiValuesSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.spatial.search.aggregations.GeoLineAggregationBuilder.SORT_FIELD;

/**
 * Time-series aware geo_line aggregation relies on TSID and time-ordered data.
 * It does no sorting, and is therefor also able to maintain a single collector of fixed memory size,
 * and re-use this over and over for each bucket collected. The total memory used will be the max bucket size
 * multiplied by the memory cost of the internal SimplifiablePoint class, and the memory required by the underlying
 * geometry simplifier to maintain two collections of this data, one array pre-sized to the max bucket size and ordered
 * by the native time-series order (timestamp descending), and another PriorityQueue ordered by increasing cost/consequence
 * of removing a point from the collection (we remove points that have the least impact on the resulting geo_line).
 * <p>
 * Whenever the bucket id changes, the simplifier memory is copied into a new InternalGeoLine aggregation object
 * and the memory re-used for the next bucket. The last bucket is compied into InternalGeoLine in the doPostCollection
 */
class TimeSeriesGeoLineBuckets {
    private static final long NO_BUCKET = -1;
    private final GeoLineMultiValuesSource valuesSources;
    private final int bucketSize;
    private final Function<Long, InternalGeoLine> geoLineBuilder;
    private long currentBucket;
    private final Simplifier simplifier;
    private final HashMap<Long, InternalAggregation> geoLines = new HashMap<>();

    /**
     * The time-series geo_line aggregation does not need to know of the sort order, because it relies entirely upon
     * the TSDB time-order (descending), and re-ordering to ascending (if required) is only needed at InternalGeoLine creation time.
     * @param bucketSize The maximum geo_line length to create, passed into the StreamingGeometrySimplifier
     * @param valuesSources For reading the geo_point and timestamp fields from each document
     * @param geoLineBuilder A function to create the InternalGeoLine with appropriate metadata (and sort order)
     * @param circuitBreaker A function to request memory for each geo_line created
     */
    TimeSeriesGeoLineBuckets(
        int bucketSize,
        GeoLineMultiValuesSource valuesSources,
        Function<Long, InternalGeoLine> geoLineBuilder,
        Function<Long, Long> circuitBreaker
    ) {
        this.valuesSources = valuesSources;
        this.bucketSize = bucketSize;
        this.geoLineBuilder = geoLineBuilder;
        this.currentBucket = NO_BUCKET;
        this.simplifier = new TimeSeriesGeoLineBuckets.Simplifier(bucketSize, circuitBreaker);
    }

    /**
     * Produce the leaf collector for collecting the timestamp (sort field) and geo_point for each document.
     * Each lucene segment will have its own leaf collector, but it is still possible to have multiple buckets per-leaf.
     */
    Leaf forLeaf(AggregationExecutionContext aggCtx) throws IOException {
        return new Leaf(aggCtx.getLeafReaderContext());
    }

    void doPostCollection() {
        // Ensure any last bucket is completed
        flushBucket(NO_BUCKET);
    }

    void doClose() {
        simplifier.reset();
    }

    /**
     * Since there is a different instance of the leaf collector for each segment, and within a segment
     * the bucket id can change (or tsid can change), we need to copy the streaming geometry simplifier memory
     * into an InternalGeoLine object before collecting the next bucket. This is handled by the Leaf.collect(doc,bucket)
     * method calling flushBucket(bucket) and any change in bucket id will copy the memory and prepare the simplifier
     * for the next bucket.
     */
    private void flushBucket(long bucket) {
        if (bucket != currentBucket) {
            if (currentBucket != NO_BUCKET) {
                if (geoLines.containsKey(currentBucket)) {
                    throw new IllegalStateException("Geoline already exists for bucket " + currentBucket);
                }
                geoLines.put(currentBucket, geoLineBuilder.apply(currentBucket));
                simplifier.reset();
            }
            currentBucket = bucket;
        }
    }

    /**
     * Build the aggregation based on saved state from the collector phase.
     * Note that the collector phase does not know about sort-order, so if the order is not the native time-series
     * order of 'descending', this is where we can do an efficient in-place reverse ordering.
     * Also note that the collector phase will re-use the same memory (in the StreamingGeometrySimplifier) for each bucket,
     * so it is important that this method is called to copy that 'scratch' memory into the final InternalGeoLine aggregation
     * objects between each bucket collection phase.
     */
    InternalGeoLine buildAggregation(long bucket, String name, Map<String, Object> metadata, boolean includeSorts, SortOrder sortOrder) {
        LineStream line = simplifier.produce();
        boolean complete = simplifier.length() < bucketSize;
        if (line.isEmpty()) {
            // TODO: Perhaps we should return an empty geo_line here?
            throw new IllegalStateException("Invalid bucket " + bucket + " for geo_line");
        }
        // TODO: For TSID perhaps we should save sortValues as longs
        double[] sortVals = line.sortValues;
        long[] bucketLine = line.encodedPoints;
        if (sortOrder == SortOrder.ASC) {
            // time-series is natively sorted DESC, so we need to reverse the order
            ArrayUtils.reverseSubArray(sortVals, 0, sortVals.length);
            ArrayUtils.reverseSubArray(bucketLine, 0, bucketLine.length);
        }
        return new InternalGeoLine(name, bucketLine, sortVals, metadata, complete, includeSorts, sortOrder, bucketSize);
    }

    /**
     * In the aggregation phase we need to return the InternalGeoLine aggregation objects for each bucket.
     * Since the collect phase was re-using the same memory for each bucket during collection, the construction
     * of all InternalGeoLine aggregations was done at the end of each bucket collection, and here we merely
     * return the results.
     */
    public InternalAggregation getGeolineForBucket(long bucket) {
        return geoLines.get(bucket);
    }

    /**
     * Wrapper for points and sort-fields that are also usable in the GeometrySimplifier library.
     * Since that library has no knowledge of timestamps or sort-fields, we need to use these custom objects
     * to maintain the geo_point-timestamp correlation through the simplification process.
     */
    private static class SimplifiablePoint extends StreamingGeometrySimplifier.PointError {
        private double sortValue;
        private long encoded;

        private SimplifiablePoint(int index, double x, double y, double sortValue) {
            super(index, x, y);
            this.sortValue = sortValue;
            setEncoded(x, y);
        }

        /**
         * The streaming geometry simplifier needs to be able to re-use objects to save memory.
         */
        private StreamingGeometrySimplifier.PointError reset(int index, double x, double y, double sortValue) {
            super.reset(index, x, y);
            this.sortValue = sortValue;
            setEncoded(x, y);
            return this;
        }

        private void setEncoded(double x, double y) {
            this.encoded = (((long) GeoEncodingUtils.encodeLongitude(x)) << 32) | GeoEncodingUtils.encodeLatitude(y) & 0xffffffffL;
        }
    }

    /**
     * Controlled memory version of Line from streaming data.
     * The points array is a re-used array sized to the maximum allowed geo_line length. The length field is the actual
     * length of the geo_line, which could be much shorter. This class allocates the two arrays used internally in
     * the InternalGeoLine aggregation object, and copies the reusable memory into these arrays.
     */
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
    }

    /**
     * Wrapping the Streaming GeometrySimplifier allowing the aggregation to extract expected points
     * and their sort fields after simplification. This class works with the SimplifiablePoint instances
     * to maintain the correlation between geo_point and the timestamp field.
     */
    static class Simplifier extends StreamingGeometrySimplifier<LineStream>
        implements
            StreamingGeometrySimplifier.PointConstructor,
            StreamingGeometrySimplifier.PointResetter {
        double currentSortValue;
        private final Function<Long, Long> circuitBreaker;

        Simplifier(int maxPoints, Function<Long, Long> circuitBreaker) {
            super("GeoLineTSDB", maxPoints, SimplificationErrorCalculator.TRIANGLE_AREA, null);
            this.pointConstructor = this;
            this.pointResetter = this;
            this.circuitBreaker = circuitBreaker;
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
            circuitBreaker.apply((Double.SIZE + Long.SIZE) * (long) length());
            return new LineStream(this.length, this.points);
        }

        public int length() {
            return length;
        }
    }

    protected class Leaf {
        private final SortedNumericDoubleValues docSortValues;
        private final MultiGeoPointValues docGeoPointValues;

        protected Leaf(LeafReaderContext ctx) throws IOException {
            // TODO: Should the sort field be hard-coded for time-series? Or just validated earlier on?
            docSortValues = valuesSources.getNumericField(SORT_FIELD.getPreferredName(), ctx);
            docGeoPointValues = valuesSources.getGeoPointField(GeoLineAggregationBuilder.POINT_FIELD.getPreferredName(), ctx);
        }

        private boolean loadSortField(int doc) throws IOException {
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
            // The consume method will rely on the newPoint and resetPoint methods to capture the sort-field value
            simplifier.consume(point.getX(), point.getY());
        }

        public void collect(int doc, long bucket) throws IOException {
            flushBucket(bucket);
            if (loadSortField(doc)) {
                loadPointField(doc);
            }
        }
    }
}
