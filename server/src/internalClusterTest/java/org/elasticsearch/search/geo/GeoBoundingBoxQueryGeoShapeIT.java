/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class GeoBoundingBoxQueryGeoShapeIT extends AbstractGeoBoundingBoxQueryIT {

    @Override
    public XContentBuilder getMapping(Version version) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("location").field("type", "geo_shape");
        if (version.before(Version.V_8_0_0) && randomBoolean()) {
            xContentBuilder.field("strategy", "recursive");
        }
        xContentBuilder.endObject().endObject().endObject().endObject();
        return xContentBuilder;
    }
}

