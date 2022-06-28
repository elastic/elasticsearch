/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class LegacyMapperServiceTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testIndexMetadataUpdateDoesNotLoseDefaultMapper() throws IOException {
        final IndexService indexService = createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_6_3_0).build()
        );
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject(MapperService.DEFAULT_MAPPING);
                {
                    builder.field("date_detection", false);
                }
                builder.endObject();
            }
            builder.endObject();
            final PutMappingRequest putMappingRequest = new PutMappingRequest();
            putMappingRequest.indices("test");
            putMappingRequest.type(MapperService.DEFAULT_MAPPING);
            putMappingRequest.source(builder);
            client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(builder).get();
        }
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
        final Settings zeroReplicasSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(zeroReplicasSettings).get();
        /*
         * This assertion is a guard against a previous bug that would lose the default mapper when applying a metadata update that did not
         * update the default mapping.
         */
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
    }

    public void testDefaultMappingIsDeprecatedOn6() throws IOException {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_6_3_0).build();
        final String mapping;
        try (XContentBuilder defaultMapping = XContentFactory.jsonBuilder()) {
            defaultMapping.startObject();
            {
                defaultMapping.startObject("_default_");
                {

                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();
            mapping = Strings.toString(defaultMapping);
        }
        final MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.merge("_default_", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertWarnings(MapperService.DEFAULT_MAPPING_ERROR_MESSAGE);
    }

}
