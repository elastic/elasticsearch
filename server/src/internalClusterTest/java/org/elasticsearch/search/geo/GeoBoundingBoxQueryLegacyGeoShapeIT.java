/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class GeoBoundingBoxQueryLegacyGeoShapeIT extends GeoBoundingBoxQueryIntegTestCase {

    @Override
    public XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location").field("type", "geo_shape").field("strategy", "recursive")
            .endObject().endObject().endObject().endObject();
    }
}

