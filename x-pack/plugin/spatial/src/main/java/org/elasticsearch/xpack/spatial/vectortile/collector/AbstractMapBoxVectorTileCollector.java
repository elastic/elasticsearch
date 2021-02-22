/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile.collector;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IGeometryFilter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.JtsAdapter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TileGeomResult;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerBuild;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerParams;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.xpack.spatial.vectortile.VectorTileUtils;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.List;

abstract class AbstractMapBoxVectorTileCollector implements VectorTileCollector {

    private final IGeometryFilter acceptAllGeomFilter = geometry -> true;
    private final IUserDataConverter ignoreUserData = new UserDataIgnoreConverter();
    private final MvtLayerParams layerParams = new MvtLayerParams();
    private final GeometryFactory geomFactory = new GeometryFactory();
    private final MvtLayerProps layerProps = new MvtLayerProps();
    private final VectorTile.Tile.Layer.Builder layerBuilder;
    private final Envelope tileEnvelope;
    protected final String field;

    protected AbstractMapBoxVectorTileCollector(Envelope tileEnvelope, String field) {
        this.field = field;
        this.tileEnvelope = tileEnvelope;
        layerBuilder = MvtLayerBuild.newLayerBuilder(field, layerParams);
    }

    @Override
    public byte[] getVectorTile() {
        MvtLayerBuild.writeProps(layerBuilder, layerProps);
        // Build MVT layer
        final VectorTile.Tile.Layer layer = layerBuilder.build();
        // Add built layer to MVT
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder().addLayers(layer);
        /// Build MVT
        return tileBuilder.build().toByteArray();
    }

    protected interface MapBoxVectorTileLeafCollector {
        Geometry geometry(int docID) throws IOException;
    }

    public abstract MapBoxVectorTileLeafCollector getVectorTileLeafCollector(LeafReaderContext context);

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {

        MapBoxVectorTileLeafCollector collector = getVectorTileLeafCollector(context);
        return new LeafCollector() {


            @Override
            public void setScorer(Scorable scorer) {
            }

            @Override
            public void collect(int docID) throws IOException {
                Geometry g = collector.geometry(docID);
                g.apply(new SphericalMercatorTransformer());
                TileGeomResult tileGeom = JtsAdapter.createTileGeom(g, tileEnvelope, geomFactory, layerParams, acceptAllGeomFilter);
                // MVT tile geometry to MVT features
                final List<VectorTile.Tile.Feature> features = JtsAdapter.toFeatures(tileGeom.mvtGeoms, layerProps, ignoreUserData);
                layerBuilder.addAllFeatures(features);
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    private static class SphericalMercatorTransformer implements CoordinateSequenceFilter {

        boolean done = false;

        @Override
        public void filter(CoordinateSequence seq, int i) {
            try {
                double lon = seq.getOrdinate(i, 0);
                seq.setOrdinate(i, 0, VectorTileUtils.lonToSphericalMercator(lon));
                double lat = seq.getOrdinate(i, 1);
                seq.setOrdinate(i, 1, VectorTileUtils.latToSphericalMercator(lat));
            } catch (Exception w) {

            }
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public boolean isGeometryChanged() {
            return true;
        }

    }

}
