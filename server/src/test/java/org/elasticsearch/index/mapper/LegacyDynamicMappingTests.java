/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class LegacyDynamicMappingTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testTypeNotCreatedOnIndexFailure() throws IOException {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_6_3_0).build();
        try (XContentBuilder mapping = jsonBuilder()) {
            mapping.startObject();
            {
                mapping.startObject("_default_");
                {
                    mapping.field("dynamic", "strict");
                }
                mapping.endObject();
            }
            mapping.endObject();
            createIndex("test", settings, "_default_", mapping);
        }
        try (XContentBuilder sourceBuilder = jsonBuilder().startObject().field("test", "test").endObject()) {
            expectThrows(
                StrictDynamicMappingException.class,
                () -> client().prepareIndex().setIndex("test").setType("type").setSource(sourceBuilder).get()
            );

            GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
            assertNull(getMappingsResponse.getMappings().get("test").get("type"));
        }
    }

}
