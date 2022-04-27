/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.Version;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoBoundingBoxQueryIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class GeoBoundingBoxQueryLegacyGeoShapeWithDocValuesIT extends GeoBoundingBoxQueryIntegTestCase {

    @Override
    protected boolean addMockGeoShapeFieldMapper() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    @Override
    public XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape")
            .field("strategy", "recursive")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    @Override
    public Version randomSupportedVersion() {
        return VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
    }
}
