/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A single line string representing a sorted sequence of geo-points
 */
public class InternalGeoLine extends InternalAggregation {
    private static final Logger logger = LogManager.getLogger(InternalGeoLine.class);
    private static final double SCALE = Math.pow(10, 6);

    private long[] line;
    private double[] sortVals;
    private int length;
    private boolean complete;
    private boolean includeSorts;

    InternalGeoLine(String name, long[] line, double[] sortVals, int length, Map<String, Object> metadata, boolean complete,
                    boolean includeSorts) {
        super(name, metadata);
        this.line = line;
        this.sortVals = sortVals;
        this.length = length;
        this.complete = complete;
        this.includeSorts = includeSorts;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoLine(StreamInput in) throws IOException {
        super(in);
        this.line = in.readLongArray();
        this.length = in.readVInt();
        this.complete = in.readBoolean();
        this.includeSorts = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLongArray(line);
        out.writeVInt(length);
        out.writeBoolean(complete);
        out.writeBoolean(includeSorts);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        int mergedSize = 0;
        boolean complete = true;
        boolean includeSorts = true;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoLine geoLine = (InternalGeoLine) aggregation;
            mergedSize += geoLine.length;
            complete &= geoLine.complete;
            includeSorts &= geoLine.includeSorts;
        }

        complete &= mergedSize <= 10000;

        long[] finalList = new long[mergedSize];
        double[] finalSortVals = new double[mergedSize];
        int idx = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoLine geoLine = (InternalGeoLine) aggregation;
            for (int i = 0; i < geoLine.length; i++) {
                finalSortVals[idx] = geoLine.sortVals[i];
                finalList[idx] = geoLine.line[i];
                idx += 1;
            }
        }

        new PathArraySorter(finalList, finalSortVals, mergedSize).sort();
        long[] finalCappedList = Arrays.copyOf(finalList, Math.min(10000, mergedSize));
        double[] finalCappedSortVals = Arrays.copyOf(finalSortVals, Math.min(10000, mergedSize));
        return new InternalGeoLine(name, finalCappedList, finalCappedSortVals, mergedSize, getMetadata(), complete, includeSorts);
    }

    @Override
    public String getWriteableName() {
        return GeoLineAggregationBuilder.NAME;
    }

    public long[] line() {
        return line;
    }

    public int length() {
        return length;
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final List<double[]> coordinates = new ArrayList<>();
        for (int i = 0; i < length; i++) {
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
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Object getProperty(List<String> path) {
        logger.error("what in the world");
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return line;
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }
}
