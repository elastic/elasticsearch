/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.spatial.search.aggregations.GeoShapeMetricAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A single line string representing a sorted sequence of geo-points
 */
public class InternalGeoLine extends InternalAggregation implements GeoShapeMetricAggregation {
    private static final double SCALE = Math.pow(10, 6);

    private long[] line;
    private double[] sortVals;
    private boolean complete;
    private boolean includeSorts;
    private SortOrder sortOrder;
    private int size;

    /**
     * A geo_line representing the bucket for a {@link GeoLineAggregationBuilder}. The values of <code>line</code> and <code>sortVals</code>
     * are expected to be sorted using <code>sortOrder</code>.
     *
     * @param name            the name of the aggregation
     * @param line            the ordered geo-points representing the line
     * @param sortVals        the ordered sort-values associated with the points in the line (e.g. timestamp)
     * @param metadata        the aggregation's metadata
     * @param complete        true iff the line is representative of all the points that fall within the bucket. False otherwise.
     * @param includeSorts    true iff the sort-values should be rendered in xContent as properties of the line-string. False otherwise.
     * @param sortOrder       the {@link SortOrder} for the line. Whether the points are to be plotted in asc or desc order
     * @param size            the max length of the line-string.
     */
    InternalGeoLine(String name, long[] line, double[] sortVals, Map<String, Object> metadata, boolean complete,
                    boolean includeSorts, SortOrder sortOrder, int size) {
        super(name, metadata);
        this.line = line;
        this.sortVals = sortVals;
        this.complete = complete;
        this.includeSorts = includeSorts;
        this.sortOrder = sortOrder;
        this.size = size;
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
        this.size = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLongArray(line);
        out.writeDoubleArray(sortVals);
        out.writeBoolean(complete);
        out.writeBoolean(includeSorts);
        sortOrder.writeTo(out);
        out.writeVInt(size);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        int mergedSize = 0;
        boolean complete = true;
        boolean includeSorts = true;
        List<InternalGeoLine> internalGeoLines = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoLine geoLine = (InternalGeoLine) aggregation;
            internalGeoLines.add(geoLine);
            mergedSize += geoLine.line.length;
            complete &= geoLine.complete;
            includeSorts &= geoLine.includeSorts;
        }
        complete &= mergedSize <= size;
        int finalSize = Math.min(mergedSize, size);

        MergedGeoLines mergedGeoLines = new MergedGeoLines(internalGeoLines, finalSize, sortOrder);
        mergedGeoLines.merge();
        // the final reduce should always be in ascending order
        if (reduceContext.isFinalReduce() && SortOrder.DESC.equals(sortOrder)) {
            new PathArraySorter(mergedGeoLines.getFinalPoints(), mergedGeoLines.getFinalSortValues(), SortOrder.ASC).sort();
        }
        return new InternalGeoLine(name, mergedGeoLines.getFinalPoints(), mergedGeoLines.getFinalSortValues(), getMetadata(), complete,
            includeSorts, sortOrder, size);
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
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
        return line == null ? 0 : line.length;
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

    public int size() {
        return size;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder
            .field("type", "Feature")
            .field("geometry", geoJSONGeometry())
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
        return Objects.hash(super.hashCode(), Arrays.hashCode(line), Arrays.hashCode(sortVals), complete, includeSorts, sortOrder, size);
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
            && Objects.equals(sortOrder, that.sortOrder)
            && Objects.equals(size, that.size);

    }

    @Override
    public Map<String, Object> geoJSONGeometry() {
        final List<double[]> coordinates = new ArrayList<>();
        for (int i = 0; i < line.length; i++) {
            int x = (int) (line[i] >> 32);
            int y = (int) line[i];
            coordinates.add(new double[] {
                roundDegrees(GeoEncodingUtils.decodeLongitude(x)),
                roundDegrees(GeoEncodingUtils.decodeLatitude(y))
            });
        }
        final Map<String, Object> geoJSON = new HashMap<>();
        geoJSON.put("type", "LineString");
        geoJSON.put("coordinates", coordinates.toArray());
        return geoJSON;
    }
}
