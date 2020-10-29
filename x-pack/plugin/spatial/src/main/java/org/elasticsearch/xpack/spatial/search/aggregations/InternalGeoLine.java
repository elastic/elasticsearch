/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A single line string representing a sorted sequence of geo-points
 */
public class InternalGeoLine extends InternalAggregation {
    private static final double SCALE = Math.pow(10, 6);

    private long[] line;
    private double[] sortVals;
    private boolean complete;
    private boolean includeSorts;
    private SortOrder sortOrder;

    InternalGeoLine(String name, long[] line, double[] sortVals, Map<String, Object> metadata, boolean complete,
                    boolean includeSorts, SortOrder sortOrder) {
        super(name, metadata);
        this.line = line;
        this.sortVals = sortVals;
        this.complete = complete;
        this.includeSorts = includeSorts;
        this.sortOrder = sortOrder;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoLine(StreamInput in) throws IOException {
        super(in);
        this.line = in.readLongArray();
        this.sortVals = in.readDoubleArray();
        this.complete = in.readBoolean();
        this.includeSorts = in.readBoolean();
        this.sortOrder = SortOrder.readFromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLongArray(line);
        out.writeDoubleArray(sortVals);
        out.writeBoolean(complete);
        out.writeBoolean(includeSorts);
        sortOrder.writeTo(out);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        int mergedSize = 0;
        boolean complete = true;
        boolean includeSorts = true;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoLine geoLine = (InternalGeoLine) aggregation;
            mergedSize += geoLine.line.length;
            complete &= geoLine.complete;
            includeSorts &= geoLine.includeSorts;
        }

        complete &= mergedSize <= GeoLineAggregator.MAX_PATH_SIZE;

        long[] finalList = new long[mergedSize];
        double[] finalSortVals = new double[mergedSize];
        int idx = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoLine geoLine = (InternalGeoLine) aggregation;
            for (int i = 0; i < geoLine.line.length; i++) {
                finalSortVals[idx] = geoLine.sortVals[i];
                finalList[idx] = geoLine.line[i];
                idx += 1;
            }
        }
        // the final reduce should always be in ascending order

        new PathArraySorter(finalList, finalSortVals, SortOrder.ASC).sort();
        long[] finalCappedList = Arrays.copyOf(finalList, Math.min(GeoLineAggregator.MAX_PATH_SIZE, mergedSize));
        double[] finalCappedSortVals = Arrays.copyOf(finalSortVals, Math.min(GeoLineAggregator.MAX_PATH_SIZE, mergedSize));
        return new InternalGeoLine(name, finalCappedList, finalCappedSortVals, getMetadata(), complete, includeSorts, sortOrder);
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public String getWriteableName() {
        return GeoLineAggregationBuilder.NAME;
    }

    public long[] line() {
        return line;
    }

    public double[] sortVals() {
        return sortVals;
    }

    public int length() {
        return line.length;
    }

    public boolean isComplete() {
        return complete;
    }

    public boolean includeSorts() {
        return includeSorts;
    }

    public SortOrder sortOrder() {
        return sortOrder;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final List<double[]> coordinates = new ArrayList<>();
        for (int i = 0; i < line.length; i++) {
            int x = (int) (line[i] >> 32);
            int y = (int) line[i];
            coordinates.add(new double[] {
                roundDegrees(GeoEncodingUtils.decodeLongitude(x)),
                roundDegrees(GeoEncodingUtils.decodeLatitude(y))
            });
        }
        builder
            .field("type", "Feature")
            .startObject("geometry")
                .field("type", "LineString")
                .array("coordinates", coordinates.toArray())
            .endObject()
            .startObject("properties")
                .field("complete", isComplete());
        if (includeSorts) {
            builder.field("sort_values", sortVals);
        }
        builder.endObject();
        return builder;
    }

    private double roundDegrees(double degree) {
        return Math.round(degree * SCALE) / SCALE;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return line;
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(line), Arrays.hashCode(sortVals), complete, includeSorts, sortOrder);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalGeoLine that = (InternalGeoLine) obj;
        return super.equals(obj)
            && Arrays.equals(line, that.line)
            && Arrays.equals(sortVals, that.sortVals)
            && Objects.equals(complete, that.complete)
            && Objects.equals(includeSorts, that.includeSorts)
            && Objects.equals(sortOrder, that.sortOrder);

    }
}
