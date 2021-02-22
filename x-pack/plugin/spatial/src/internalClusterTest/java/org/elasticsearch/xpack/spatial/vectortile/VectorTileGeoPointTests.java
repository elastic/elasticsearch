/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class VectorTileGeoPointTests extends VectorTileTests {

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        return XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(defaultFieldName)
            .field("type", "geo_point")
            .endObject().endObject().endObject();
    }
}
