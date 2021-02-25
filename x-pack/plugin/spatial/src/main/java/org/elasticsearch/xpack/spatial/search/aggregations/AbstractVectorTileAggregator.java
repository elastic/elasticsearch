/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractVectorTileAggregator extends MetricsAggregator {

    protected final ValuesSource valuesSource;
    protected final int x;
    protected final int y;
    protected final int z;
    protected final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();

    public AbstractVectorTileAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int z,
        int x,
        int y,
        int extent,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSourceConfig.getValuesSource();
        this.z = z;
        this.x = x;
        this.y = y;
        layerBuilder.setVersion(2);
        // TODO: maybe we should use a static name here
        layerBuilder.setName(name);
        layerBuilder.setExtent(extent);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }


    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || layerBuilder.getFeaturesCount() == 0) {
            return buildEmptyAggregation();
        }
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
        // Build MVT layer
        final VectorTile.Tile.Layer layer = layerBuilder.build();
        // Add built layer to MVT
        tileBuilder.addLayers(layer);
        /// Build MVT
        return new InternalVectorTile(name, tileBuilder.build().toByteArray(), metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalVectorTile(name, new byte[0], metadata());
    }

    @Override
    public void doClose() {
    }
}
