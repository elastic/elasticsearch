/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.Version;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class GeoBoundingBoxQueryGeoPointIT extends GeoBoundingBoxQueryIntegTestCase {

    @SuppressWarnings("deprecation")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(TestGeoShapeFieldMapperPlugin.class);
    }

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
    public Version randomSupportedVersion() throws IOException {
        return VersionUtils.randomIndexCompatibleVersion(random());
    }
}
