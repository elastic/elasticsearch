/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;


import com.wdtinc.mapbox_vector_tile.adapt.jts.MvtReader;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TagIgnoreConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.model.JtsLayer;
import com.wdtinc.mapbox_vector_tile.adapt.jts.model.JtsMvt;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.spatial.action.VectorTileAction;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.ByteArrayInputStream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class VectorTileGeoShapeTests extends VectorTileTests {

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        return XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(defaultFieldName)
            .field("type", "geo_shape")
            .endObject().endObject().endObject();
    }

    public void testVectorTileWithPolygons() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultFieldName, "POLYGON((0 0, 30 0, 30 30, 0 30, 0 0))")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultFieldName, "POLYGON((0 0, 30 0, 30 30, 0 30, 0 0))")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        VectorTileAction.Response r = client().execute(VectorTileAction.INSTANCE,
            new VectorTileAction.Request(new String[] {defaultIndexName}, defaultFieldName, 0, 0, 0)).get();
        byte[] b = r.getVectorTiles();
        GeometryFactory geomFactory = new GeometryFactory();
        JtsMvt jtsMvt = MvtReader.loadMvt(new ByteArrayInputStream(b), geomFactory, new TagIgnoreConverter());
        JtsLayer layer = jtsMvt.getLayer(defaultFieldName);
        assertEquals(2, layer.getGeometries().size());
    }
}
