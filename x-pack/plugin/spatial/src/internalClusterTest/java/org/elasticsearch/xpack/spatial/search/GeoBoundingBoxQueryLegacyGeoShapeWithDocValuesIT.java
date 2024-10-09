/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoBoundingBoxQueryIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

@UpdateForV9(owner = UpdateForV9.Owner.SEARCH_ANALYTICS)
@LuceneTestCase.AwaitsFix(bugUrl = "this is testing legacy functionality so can likely be removed in 9.0")
public class GeoBoundingBoxQueryLegacyGeoShapeWithDocValuesIT extends GeoBoundingBoxQueryIntegTestCase {

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
    public IndexVersion randomSupportedVersion() {
        return IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0);
    }
}
