/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.spatial.vectortile.VectorTileUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Base Vector tile aggregator class. It might generate up to three layers: If there is any point it generates a POINT layer
 * that cluster points in a cluster them in a 256 X 256 grid. If there is any line, it generated a LINE layer with a 4096 extent
 * contains ing all lines. If there is any polygon, it generated a POLYGON layer with a 4096 extent contains ing all lines.
 */
public abstract class AbstractVectorTileAggregator extends MetricsAggregator {

    protected static final int POINT_EXTENT = 256;
    protected static final String POINT_LAYER = "POINT";
    protected static final int LINE_EXTENT = 4096;
    protected static final String LINE_LAYER = "LINE";
    protected static final int POLYGON_EXTENT = 4096;
    protected static final String POLYGON_LAYER = "POLYGON";

    protected static final String ID_TAG = "id";

    protected final ValuesSource valuesSource;
    protected final int x;
    protected final int y;
    protected final int z;
    private VectorTile.Tile.Layer.Builder polygonLayerBuilder;
    private int numPolygons;
    private VectorTile.Tile.Layer.Builder lineLayerBuilder;
    private int numLines;
    private LongArray clusters;
    private final double pointXScale;
    private final double pointYScale;
    protected final Rectangle rectangle;
    final VectorTile.Tile.Value.Builder valueBuilder = VectorTile.Tile.Value.newBuilder();

    public AbstractVectorTileAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int z,
        int x,
        int y,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSourceConfig.getValuesSource();
        this.z = z;
        this.x = x;
        this.y = y;
        this.rectangle = VectorTileUtils.getTileBounds(z, x, y);
        this.pointXScale = 1d / ((rectangle.getMaxLon() - rectangle.getMinLon()) / (double) POINT_EXTENT);
        this.pointYScale = -1d / ((rectangle.getMaxLat() - rectangle.getMinLat()) / (double) POINT_EXTENT);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    protected void addPoint(double lat, double lon) {
        final int x = (int) (pointXScale * (VectorTileUtils.lonToSphericalMercator(lon) - rectangle.getMinX()));
        final int y = (int) (pointYScale * (VectorTileUtils.latToSphericalMercator(lat) - rectangle.getMinY())) + POINT_EXTENT;
        if (x >= 0 && x < POINT_EXTENT && y >= 0 && y < POINT_EXTENT) {
            if (clusters == null) {
                clusters = bigArrays().newLongArray(POINT_EXTENT * POINT_EXTENT);
            }
            final int pos = POINT_EXTENT * x + y;
            clusters.increment(pos, 1);
        }
    }

    protected void addLineFeatures(String id, List<VectorTile.Tile.Feature> features) {
        for (VectorTile.Tile.Feature feature : features) {
            if (lineLayerBuilder == null) {
                lineLayerBuilder = VectorTile.Tile.Layer.newBuilder();
                lineLayerBuilder.setVersion(2);
                lineLayerBuilder.setName(LINE_LAYER);
                lineLayerBuilder.setExtent(LINE_EXTENT);
                lineLayerBuilder.addKeys(ID_TAG);
            }
            valueBuilder.clear();
            valueBuilder.setStringValue(id);
            lineLayerBuilder.addValues(valueBuilder);
            VectorTile.Tile.Feature.Builder builder = feature.toBuilder();
            builder.addTags(0);
            builder.addTags(numLines++);
            lineLayerBuilder.addFeatures(builder);
        }
    }

    protected void addPolygonFeatures(String id, List<VectorTile.Tile.Feature> features) {
        for (VectorTile.Tile.Feature feature : features) {
            if (polygonLayerBuilder == null) {
                polygonLayerBuilder = VectorTile.Tile.Layer.newBuilder();
                polygonLayerBuilder.setVersion(2);
                polygonLayerBuilder.setName(POLYGON_LAYER);
                polygonLayerBuilder.setExtent(POLYGON_EXTENT);
                polygonLayerBuilder.addKeys(ID_TAG);
            }
            valueBuilder.clear();
            valueBuilder.setStringValue(id);
            polygonLayerBuilder.addValues(valueBuilder);
            VectorTile.Tile.Feature.Builder builder = feature.toBuilder();
            builder.addTags(0);
            builder.addTags(numPolygons++);
            polygonLayerBuilder.addFeatures(builder);
        }
    }



    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null) {
            return buildEmptyAggregation();
        }
        final VectorTile.Tile.Layer polygons = polygonLayerBuilder != null ? polygonLayerBuilder.build() : null;
        final VectorTile.Tile.Layer lines = lineLayerBuilder != null ? lineLayerBuilder.build() : null;
        final long[] points;
        if (clusters != null) {
            points = new long[POINT_EXTENT * POINT_EXTENT];
            for (int i = 0; i < POINT_EXTENT * POINT_EXTENT; i++) {
                points[i] = clusters.get(i);
            }
        } else {
            points = null;
        }
        return new InternalVectorTile(name, polygons, lines, points, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalVectorTile(name, null, null, null, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(clusters);
    }
}
