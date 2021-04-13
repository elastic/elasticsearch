/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.spatial.vectortile.FeatureFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalVectorTileTests extends InternalAggregationTestCase<InternalVectorTile> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new SpatialPlugin();
    }

    static VectorTile.Tile.Layer randomPolygonLayer(int shapes) {
        final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();
        layerBuilder.setVersion(2);
        layerBuilder.setName(AbstractVectorTileAggregator.POLYGON_LAYER);
        layerBuilder.setExtent(AbstractVectorTileAggregator.POLYGON_EXTENT);
        final FeatureFactory factory = new FeatureFactory(0, 0, 0, AbstractVectorTileAggregator.POLYGON_EXTENT);
        final IUserDataConverter ignoreData = new UserDataIgnoreConverter();
        for (int i =0; i < shapes; i++) {
            int count = layerBuilder.getFeaturesCount();
            while(true) {
                Geometry geometry = GeometryTestUtils.randomPolygon(false);
                List<VectorTile.Tile.Feature> features = factory.getFeatures(geometry, ignoreData);
                for (VectorTile.Tile.Feature feature : features) {
                    layerBuilder.addFeatures(feature);
                }
                if (count < layerBuilder.getFeaturesCount()) {
                    break;
                }
            }
        }
        return layerBuilder.build();
    }

    static VectorTile.Tile.Layer randomLineLayer(int shapes) {
        final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();
        layerBuilder.setVersion(2);
        layerBuilder.setName(AbstractVectorTileAggregator.POLYGON_LAYER);
        layerBuilder.setExtent(AbstractVectorTileAggregator.POLYGON_EXTENT);
        final FeatureFactory factory = new FeatureFactory(0, 0, 0, AbstractVectorTileAggregator.POLYGON_EXTENT);
        final IUserDataConverter ignoreData = new UserDataIgnoreConverter();
        for (int i =0; i < shapes; i++) {
            int count = layerBuilder.getFeaturesCount();
            while(true) {
                Geometry geometry = GeometryTestUtils.randomLine(false);
                List<VectorTile.Tile.Feature> features = factory.getFeatures(geometry, ignoreData);
                for (VectorTile.Tile.Feature feature : features) {
                    layerBuilder.addFeatures(feature);
                }
                if (count < layerBuilder.getFeaturesCount()) {
                    break;
                }
            }
        }
        return layerBuilder.build();

    }

    static long[] randomPoints() {
        long[] points  = new long[AbstractVectorTileAggregator.POINT_EXTENT * AbstractVectorTileAggregator.POINT_EXTENT];
        for( int i = 0; i < points.length;  i++) {
            points[i] = randomInt(10);
        }
        return points;
    }

    @Override
    protected InternalVectorTile createTestInstance(String name, Map<String, Object> metadata) {
        int size = randomIntBetween(5, 10);
        VectorTile.Tile.Layer polygons = randomBoolean() ? randomPolygonLayer(size) : null;
        VectorTile.Tile.Layer lines = randomBoolean() ? randomLineLayer(size) : null;
        long[] points = randomBoolean() ? randomPoints() : null;
        return new InternalVectorTile(name, polygons, lines, points, metadata);
    }

    @Override
    protected InternalVectorTile mutateInstance(InternalVectorTile instance) {
        String name = instance.getName();
        VectorTile.Tile.Layer polygons = instance.polygons;
        VectorTile.Tile.Layer lines = instance.lines;
        long[] points = instance.points;
        Map<String, Object> metadata = instance.getMetadata();
        switch (randomIntBetween(0, 4)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            case 2:
                polygons = randomPolygonLayer(randomIntBetween(10, 20));
                break;
            case 3:
                lines = randomLineLayer(randomIntBetween(10, 20));
                break;
            case 4:
                points = randomPoints();
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalVectorTile(name, polygons, lines, points, metadata);
    }

    @Override
    protected List<InternalVectorTile> randomResultsToReduce(String name, int size) {
        List<InternalVectorTile> instances = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            instances.add(createTestInstance(name, null));
        }
        return instances;
    }

    @Override
    protected void assertReduced(InternalVectorTile reduced, List<InternalVectorTile> inputs) {
        int numPolygons = 0;
        int numLines = 0;
        long numPoints = 0;
        for (InternalVectorTile input : inputs) {
            if (input.polygons != null) {
                numPolygons += input.polygons.getFeaturesCount();
            }
            if (input.lines != null) {
                numLines += input.lines.getFeaturesCount();
            }
            if (input.points != null) {
                numPoints += Arrays.stream(input.points).sum();
            }
        }
        int numReducedPolygons = reduced.polygons != null ? reduced.polygons.getFeaturesCount() : 0;
        assertThat(numReducedPolygons, Matchers.equalTo(numPolygons));
        int numReducedLines = reduced.lines != null ? reduced.lines.getFeaturesCount() : 0;
        assertThat(numReducedLines, Matchers.equalTo(numLines));
        long numReducedPoints = reduced.points != null ? Arrays.stream(reduced.points).sum() : 0;
        assertThat(numReducedPoints, Matchers.equalTo(numPoints));
    }

    @Override
    protected void assertFromXContent(InternalVectorTile aggregation, ParsedAggregation parsedAggregation) throws IOException {
        // The output is binary so nothing to do here
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(super.getNamedXContents(), new NamedXContentRegistry.Entry(Aggregation.class,
                new ParseField(VectorTileAggregationBuilder.NAME),
                (p, c) -> {
                    assumeTrue("There is no ParsedVectorTile yet", false);
                    return null;
                }
        ));
    }
}
