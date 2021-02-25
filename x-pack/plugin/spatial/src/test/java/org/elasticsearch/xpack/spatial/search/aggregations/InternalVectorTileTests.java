/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.google.protobuf.InvalidProtocolBufferException;
import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.spatial.vectortile.FeatureFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalVectorTileTests extends InternalAggregationTestCase<InternalVectorTile> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new SpatialPlugin();
    }

    static VectorTile.Tile randomVectorTile(int shapes) {
        final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();
        layerBuilder.setVersion(2);
        layerBuilder.setName(randomAlphaOfLength(15));
        // TODO: It would be nice to use different tiles?
        final FeatureFactory factory = new FeatureFactory(0, 0, 0, 4096);
        for (int i =0; i < shapes; i++) {
            int count = layerBuilder.getFeaturesCount();
            while(true) {
                Geometry geometry = GeometryTestUtils.randomPolygon(false);
                List<VectorTile.Tile.Feature> features = factory.getFeatures(geometry);
                for (VectorTile.Tile.Feature feature : features) {
                    layerBuilder.addFeatures(feature);
                }
                if (count < layerBuilder.getFeaturesCount()) {
                    break;
                }
            }
        }
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
        // Build MVT layer
        final VectorTile.Tile.Layer layer = layerBuilder.build();
        // Add built layer to MVT
        tileBuilder.addLayers(layer);
        return tileBuilder.build();
    }

    @Override
    protected InternalVectorTile createTestInstance(String name, Map<String, Object> metadata) {
        int size = randomIntBetween(10, 20);
        VectorTile.Tile tile = randomVectorTile(size);
        return new InternalVectorTile(name, tile.toByteArray(), metadata);
    }

    @Override
    protected InternalVectorTile mutateInstance(InternalVectorTile instance) {
        String name = instance.getName();
        byte[] tile = instance.getVectorTile();
        Map<String, Object> metadata = instance.getMetadata();
        switch (randomIntBetween(0, 2)) {
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
                tile = randomVectorTile(randomIntBetween(10, 20)).toByteArray();
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalVectorTile(name,tile, metadata);
    }

    @Override
    protected List<InternalVectorTile> randomResultsToReduce(String name, int size) {
        List<InternalVectorTile> instances = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // use the magicDecimal to have absolute ordering between heap-sort and testing array sorting
            instances.add(new InternalVectorTile(name, randomVectorTile(randomIntBetween(10, 20)).toByteArray(), null));
        }
        return instances;
    }

    @Override
    protected void assertReduced(InternalVectorTile reduced, List<InternalVectorTile> inputs) {
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
        for (InternalAggregation aggregation : inputs) {
            try {
                tileBuilder.mergeFrom(((InternalVectorTile) aggregation).getVectorTile());
            } catch (InvalidProtocolBufferException ex) {
                fail(ex.getMessage());
            }
        }
        assertArrayEquals(reduced.getVectorTile(), tileBuilder.build().toByteArray());
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
