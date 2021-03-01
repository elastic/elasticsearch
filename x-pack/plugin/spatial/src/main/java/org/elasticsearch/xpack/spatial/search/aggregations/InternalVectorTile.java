/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmd;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalVectorTile extends InternalAggregation {
    protected final VectorTile.Tile.Layer polygons;
    protected final VectorTile.Tile.Layer lines;
    protected final long[] points;

    public InternalVectorTile(String name, VectorTile.Tile.Layer polygons,
                              VectorTile.Tile.Layer lines, long[] points, Map<String, Object> metadata) {
        super(name, metadata);
        this.polygons = polygons;
        this.lines = lines;
        this.points = points;
    }

    /**
     * Read from a stream.
     */
    public InternalVectorTile(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            final VectorTile.Tile.Layer.Builder polygon = VectorTile.Tile.Layer.newBuilder();
            this.polygons = polygon.mergeFrom(in.readByteArray()).build();
        } else {
            this.polygons = null;
        }
        if (in.readBoolean()) {
            final VectorTile.Tile.Layer.Builder line = VectorTile.Tile.Layer.newBuilder();
            this.lines = line.mergeFrom(in.readByteArray()).build();
        } else {
            this.lines = null;
        }
        this.points = in.readBoolean() ? in.readLongArray() : null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (polygons != null) {
            out.writeBoolean(true);
            out.writeByteArray(polygons.toByteArray());
        } else {
            out.writeBoolean(false);
        }
        if (lines != null) {
            out.writeBoolean(true);
            out.writeByteArray(lines.toByteArray());
        } else {
            out.writeBoolean(false);
        }
        if (points != null) {
            out.writeBoolean(true);
            out.writeLongArray(points);
        } else {
            out.writeBoolean(false);
        }

    }

    @Override
    public String getWriteableName() {
        return VectorTileAggregationBuilder.NAME;
    }

    public byte[] getVectorTile() {
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
        if (polygons != null) {
            tileBuilder.addLayers(polygons);
        }
        if (lines != null) {
            tileBuilder.addLayers(lines);
        }
        if (points != null) {
            tileBuilder.addLayers(buildPointLayer());
        }
        return tileBuilder.build().toByteArray();
    }

    private VectorTile.Tile.Layer buildPointLayer() {
        final VectorTile.Tile.Layer.Builder pointLayerBuilder = VectorTile.Tile.Layer.newBuilder();
        pointLayerBuilder.setVersion(2);
        pointLayerBuilder.setName(AbstractVectorTileAggregator.POINT_LAYER);
        pointLayerBuilder.setExtent(AbstractVectorTileAggregator.POINT_EXTENT);
        final List<Integer> commands = new ArrayList<>();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (int i = 0; i < AbstractVectorTileAggregator.POINT_EXTENT; i++) {
            int xVal = i * AbstractVectorTileAggregator.POINT_EXTENT;
            for (int j = 0; j < AbstractVectorTileAggregator.POINT_EXTENT; j++) {
                final long count  = points[xVal + j];
                if (count > 0) {
                    commands.clear();
                    commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
                    commands.add(BitUtil.zigZagEncode(i));
                    commands.add(BitUtil.zigZagEncode(j));
                    //TODO: add count as a tag?
                    featureBuilder.clear();
                    featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
                    featureBuilder.addAllGeometry(commands);
                    pointLayerBuilder.addFeatures(featureBuilder);
                }
            }
        }
        return pointLayerBuilder.build();
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        VectorTile.Tile.Layer.Builder polygons = null;
        VectorTile.Tile.Layer.Builder lines = null;
        long[] points = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalVectorTile internalVectorTile = (InternalVectorTile) aggregation;
            if (internalVectorTile.polygons != null) {
                if (polygons == null) {
                    polygons = VectorTile.Tile.Layer.newBuilder();
                    polygons.setVersion(2);
                    polygons.setName(AbstractVectorTileAggregator.POLYGON_LAYER);
                    polygons.setExtent(AbstractVectorTileAggregator.POLYGON_EXTENT);
                }
                mergeLayer(polygons, internalVectorTile.polygons);
            }
            if (internalVectorTile.lines != null) {
                if (lines == null) {
                    lines = VectorTile.Tile.Layer.newBuilder();
                    lines.setVersion(2);
                    lines.setName(AbstractVectorTileAggregator.LINE_LAYER);
                    lines.setExtent(AbstractVectorTileAggregator.LINE_EXTENT);
                }
                mergeLayer(lines, internalVectorTile.lines);
            }
            if (internalVectorTile.points != null) {
                if (points == null) {
                    points = new long[AbstractVectorTileAggregator.POINT_EXTENT *  AbstractVectorTileAggregator.POINT_EXTENT];
                }
                mergePoints(points, internalVectorTile.points);
            }
        }
        final VectorTile.Tile.Layer polygonLayer = polygons != null ? polygons.build() : null;
        final VectorTile.Tile.Layer lineLayer = lines != null ? lines.build() : null;
        return new InternalVectorTile(name, polygonLayer, lineLayer, points, getMetadata());
    }

    private void mergeLayer(VectorTile.Tile.Layer.Builder layerBuilder, VectorTile.Tile.Layer layer) {
        for(int i = 0; i < layer.getFeaturesCount(); i++) {
            layerBuilder.addFeatures(layer.getFeatures(i));
        }
    }

    private void mergePoints(long[] pointsBuilder, long[] layer) {
        for(int i = 0; i < pointsBuilder.length; i++) {
            pointsBuilder[i] += layer[i];
        }
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return getVectorTile();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), getVectorTile());
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), polygons, lines, Arrays.hashCode(points));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalVectorTile that = (InternalVectorTile) obj;
        return Arrays.equals(points, that.points) &&
            Objects.equals(polygons, that.polygons) &&
            Objects.equals(lines, that.lines);
    }
}
