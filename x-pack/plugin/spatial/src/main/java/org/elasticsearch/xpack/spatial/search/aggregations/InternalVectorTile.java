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
import org.elasticsearch.xpack.spatial.vectortile.VectorTileUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalVectorTile extends InternalAggregation {
    private static String COUNT_KEY = "count";
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
        this.polygons = in.readBoolean() ? VectorTile.Tile.Layer.parseDelimitedFrom(in) : null;
        this.lines = in.readBoolean() ? VectorTile.Tile.Layer.parseDelimitedFrom(in) : null;
        this.points = in.readBoolean() ? in.readLongArray() : null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (polygons != null) {
            out.writeBoolean(true);
            polygons.writeDelimitedTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (lines != null) {
            out.writeBoolean(true);
            lines.writeDelimitedTo(out);
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

    public byte[] getVectorTileBytes() {
        return getVectorTile().toByteArray();
    }

    public void writeTileToStream(OutputStream stream) throws IOException {
        getVectorTile().writeTo(stream);
    }

    private VectorTile.Tile getVectorTile() {
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
        return tileBuilder.build();
    }

    private VectorTile.Tile.Layer buildPointLayer() {
        final VectorTile.Tile.Layer.Builder pointLayerBuilder =
            VectorTileUtils.createLayerBuilder(AbstractVectorTileAggregator.POINT_LAYER, AbstractVectorTileAggregator.POINT_EXTENT);
        pointLayerBuilder.addKeys(COUNT_KEY);
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        final VectorTile.Tile.Value.Builder valueBuilder = VectorTile.Tile.Value.newBuilder();
        final HashMap<Long, Integer> values = new HashMap<>();
        for (int i = 0; i < AbstractVectorTileAggregator.POINT_EXTENT; i++) {
            final int xVal = i * AbstractVectorTileAggregator.POINT_EXTENT;
            for (int j = 0; j < AbstractVectorTileAggregator.POINT_EXTENT; j++) {
                final long count  = points[xVal + j];
                if (count > 0) {
                    featureBuilder.clear();
                    featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
                    // create geometry commands
                    featureBuilder.addGeometry(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
                    featureBuilder.addGeometry(BitUtil.zigZagEncode(i));
                    featureBuilder.addGeometry(BitUtil.zigZagEncode(j));
                    // Add count as key value pair
                    featureBuilder.addTags(0);
                    final int tagValue;
                    if (values.containsKey(count)) {
                        tagValue = values.get(count);
                    } else {
                        valueBuilder.clear();
                        valueBuilder.setIntValue(count);
                        tagValue = values.size();
                        pointLayerBuilder.addValues(valueBuilder);
                        values.put(count, tagValue);
                    }
                    featureBuilder.addTags(tagValue);
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
                    polygons =
                        VectorTileUtils.createLayerBuilder(AbstractVectorTileAggregator.POLYGON_LAYER,
                            AbstractVectorTileAggregator.POLYGON_EXTENT
                    );
                    polygons.addKeys(AbstractVectorTileAggregator.ID_TAG);
                }
                mergeLayer(polygons, internalVectorTile.polygons);
            }
            if (internalVectorTile.lines != null) {
                if (lines == null) {
                    lines = VectorTileUtils.createLayerBuilder(AbstractVectorTileAggregator.LINE_LAYER,
                        AbstractVectorTileAggregator.LINE_EXTENT
                    );
                    lines.addKeys(AbstractVectorTileAggregator.ID_TAG);
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
        layerBuilder.addAllKeys(layer.getKeysList());
        layerBuilder.addAllValues(layer.getValuesList());
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
            return getVectorTileBytes();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), getVectorTileBytes());
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
