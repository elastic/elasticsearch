/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.rest;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;

import org.elasticsearch.test.ESTestCase;

import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsString;

public class VectorTileUtilTests extends ESTestCase {

    public void testAddPropertyToFeature() {
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        // boolean
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "bool", true);
        assertPropertyToFeature(layerProps, featureBuilder, 1);
        // byte
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "byte", (byte) 1);
        assertPropertyToFeature(layerProps, featureBuilder, 2);
        // short
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "short", (short) 2);
        assertPropertyToFeature(layerProps, featureBuilder, 3);
        // integer
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "integer", 3);
        assertPropertyToFeature(layerProps, featureBuilder, 4);
        // long
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "long", 4L);
        assertPropertyToFeature(layerProps, featureBuilder, 5);
        // float
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "float", 5f);
        assertPropertyToFeature(layerProps, featureBuilder, 6);
        // double
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "double", 6d);
        assertPropertyToFeature(layerProps, featureBuilder, 7);
        // string
        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "string", "7");
        assertPropertyToFeature(layerProps, featureBuilder, 8);
        // invalid
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, "invalid", new Object())
        );
        assertThat(ex.getMessage(), containsString("Unsupported vector tile type for field [invalid]"));
    }

    private void assertPropertyToFeature(MvtLayerProps layerProps, VectorTile.Tile.Feature.Builder featureBuilder, int numProps) {
        assertEquals(numProps, StreamSupport.stream(layerProps.getKeys().spliterator(), false).count());
        assertEquals(numProps, StreamSupport.stream(layerProps.getVals().spliterator(), false).count());
        assertEquals(2 * numProps, featureBuilder.getTagsCount());
    }
}
