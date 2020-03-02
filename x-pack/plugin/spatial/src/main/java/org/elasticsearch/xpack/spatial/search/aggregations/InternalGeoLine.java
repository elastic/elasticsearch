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
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.util.ArrayUtil.grow;

/**
 * A single line string representing a sorted sequence of geo-points
 */
public class InternalGeoLine extends InternalAggregation {
    private static final Logger logger = LogManager.getLogger(InternalGeoLine.class);

    private long[] line;
    private double[] sortVals;
    private int length;

    InternalGeoLine(String name, long[] line, double[] sortVals, int length, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.line = line;
        this.sortVals = sortVals;
        this.length = length;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoLine(StreamInput in) throws IOException {
        super(in);
        this.line = in.readLongArray();
        this.length = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLongArray(line);
        out.writeVInt(length);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        int mergedSize = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoLine geoLine = (InternalGeoLine) aggregation;
            mergedSize += geoLine.length;
        }

        long[] finalList = new long[mergedSize];
        double[] finalSortVals = new double[mergedSize];
        int idx = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoLine geoLine = (InternalGeoLine) aggregation;
            for (int i = 0; i < geoLine.length; i++) {
                finalSortVals[idx] = geoLine.sortVals[i];
                finalList[idx++] = geoLine.line[i];
            }
        }

        new PathArraySorter(finalList, finalSortVals, length).sort();

        // sort the final list
        return new InternalGeoLine(name, finalList, finalSortVals, mergedSize, pipelineAggregators(), getMetaData());
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

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("type", "LineString");
        final List<double[]> coordinates = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            int x = (int) line[i] >> 32;
            int y = (int) line[i];
            coordinates.add(new double[] { GeoEncodingUtils.decodeLongitude(x), GeoEncodingUtils.decodeLatitude(y) });
        }

        builder.array("coordinates", coordinates.toArray());
        builder.array("sorts", sortVals);
        return builder;
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
