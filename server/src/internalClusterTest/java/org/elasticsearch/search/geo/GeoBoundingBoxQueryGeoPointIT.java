/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

public class GeoBoundingBoxQueryGeoPointIT extends GeoBoundingBoxQueryIntegTestCase {

    @Override
    public XContentBuilder getMapping() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        return xContentBuilder;
    }

    @Override
    public IndexVersion randomSupportedVersion() {
        return IndexVersionUtils.randomCompatibleWriteVersion(random());
    }
}
