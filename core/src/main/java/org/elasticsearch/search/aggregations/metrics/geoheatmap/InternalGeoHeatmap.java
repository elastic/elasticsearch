/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.geoheatmap;

import java.io.IOException;
import java.util.AbstractList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

/**
 *
 */
public class InternalGeoHeatmap extends InternalMetricsAggregation implements GeoHeatmap {

    private int gridLevel;
    private int rows;
    private int columns;
    private double minX;
    private double minY;
    private double maxX;
    private double maxY;
    private int[] counts;

    InternalGeoHeatmap(String name, int gridLevel, int rows, int columns, double minX, double minY, double maxX, double maxY, int[] counts,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.gridLevel = gridLevel;
        this.rows = rows;
        this.columns = columns;
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
        this.counts = counts;
    }

    private InternalGeoHeatmap newInternalGeoHeatmap() {
        return new InternalGeoHeatmap(name, gridLevel, rows, columns, minX, minY, maxX, maxY, new int[counts.length], pipelineAggregators(),
                metaData);
    }

    /**
     * Read from a stream.
     */
    public InternalGeoHeatmap(StreamInput in) throws IOException {
        super(in);
        this.gridLevel = in.readVInt();
        this.rows = in.readVInt();
        this.columns = in.readVInt();
        this.minX = in.readDouble();
        this.minY = in.readDouble();
        this.maxX = in.readDouble();
        this.maxY = in.readDouble();
        this.counts = in.readVIntArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(gridLevel);
        out.writeVInt(rows);
        out.writeVInt(columns);
        out.writeDouble(minX);
        out.writeDouble(minY);
        out.writeDouble(maxX);
        out.writeDouble(maxY);
        out.writeVIntArray(counts);
    }

    @Override
    public String getWriteableName() {
        return GeoHeatmapAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        InternalGeoHeatmap reduced = newInternalGeoHeatmap();
        for (InternalAggregation aggregation : aggregations) {

            assert aggregation.getName().equals(getName());
            assert aggregation instanceof InternalGeoHeatmap;
            InternalGeoHeatmap toMerge = (InternalGeoHeatmap) aggregation;
            if (toMerge.columns == 0) {
                continue;
            } else if (reduced.columns == 0) {
                reduced.gridLevel = toMerge.gridLevel;
                reduced.rows = toMerge.rows;
                reduced.columns = toMerge.columns;
                reduced.minX = toMerge.minX;
                reduced.minY = toMerge.minY;
                reduced.maxX = toMerge.maxX;
                reduced.maxY = toMerge.maxY;
                reduced.counts = toMerge.counts;
            } else {
                assert toMerge.gridLevel == reduced.gridLevel;
                assert toMerge.columns == reduced.columns;
                assert toMerge.maxX == reduced.maxX;
                assert toMerge.counts.length == reduced.counts.length;
                for (int i = 0; i < toMerge.counts.length; i++) {
                    reduced.counts[i] += toMerge.counts[i];
                }
            }
        }
        return reduced;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        List<List<Integer>> countLists = asInts2D(columns, rows, counts);
        builder.field("grid_level", gridLevel).field("rows", rows).field("columns", columns).field("min_x", minX).field("min_y", minY)
                .field("max_x", maxX).field("max_y", maxY).startArray("counts");
        for (List<Integer> row : countLists) {
            builder.startArray();
            if (row != null) {
                for (Integer i : row) {
                    builder.value(i);
                }
            }
            builder.endArray();
        }
        builder.endArray();
        return builder;
    }

    // {@see org.apache.solr.handler.component.SpatialHeatmapFacets#asInts2D}
    static List<List<Integer>> asInts2D(final int columns, final int rows, final int[] counts) {
        // Returns a view versus returning a copy. This saves memory.
        // The data is oriented naturally for human/developer viewing: one row
        // at a time top-down
        return new AbstractList<List<Integer>>() {
            @Override
            public List<Integer> get(final int rowIdx) {// top-down remember;
                                                        // the heatmap.counts is
                                                        // bottom up
                // check if all zeroes and return null if so
                boolean hasNonZero = false;
                int y = rows - rowIdx - 1;// flip direction for 'y'
                for (int c = 0; c < columns; c++) {
                    if (counts[c * rows + y] > 0) {
                        hasNonZero = true;
                        break;
                    }
                }
                if (!hasNonZero) {
                    return null;
                }

                return new AbstractList<Integer>() {
                    @Override
                    public Integer get(int columnIdx) {
                        return counts[columnIdx * rows + y];
                    }

                    @Override
                    public int size() {
                        return columns;
                    }
                };
            }

            @Override
            public int size() {
                return rows;
            }
        };
    }

    @Override
    public int getGridLevel() {
        return gridLevel;
    }

    @Override
    public int getColumns() {
        return columns;
    }

    @Override
    public int getRows() {
        return rows;
    }

    @Override
    public int[] getCounts() {
        return counts;
    }

    @Override
    public double getMinX() {
        return minX;
    }

    @Override
    public double getMinY() {
        return minY;
    }

    @Override
    public double getMaxX() {
        return maxX;
    }

    @Override
    public double getMaxY() {
        return maxY;
    }
}
