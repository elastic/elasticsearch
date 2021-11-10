/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.mapping;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class LegacyUpdateMappingIntegrationIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDefaultMappingSettings() throws Exception {
        logger.info("Creating index with _default_ mappings");
        try (XContentBuilder defaultMapping = JsonXContent.contentBuilder()) {
            defaultMapping.startObject();
            {
                defaultMapping.startObject(MapperService.DEFAULT_MAPPING);
                {
                    defaultMapping.field("date_detection", false);
                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_6_3_0).build())
                .addMapping(MapperService.DEFAULT_MAPPING, defaultMapping)
                .get();
        }

        {
            final GetMappingsResponse getResponse = client().admin()
                .indices()
                .prepareGetMappings("test")
                .addTypes(MapperService.DEFAULT_MAPPING)
                .get();
            final Map<String, Object> defaultMapping = getResponse.getMappings()
                .get("test")
                .get(MapperService.DEFAULT_MAPPING)
                .sourceAsMap();
            assertThat(defaultMapping, hasKey("date_detection"));
        }

        logger.info("Emptying _default_ mappings");
        // now remove it
        try (
            XContentBuilder mappingBuilder = JsonXContent.contentBuilder()
                .startObject()
                .startObject(MapperService.DEFAULT_MAPPING)
                .endObject()
                .endObject()
        ) {
            final AcknowledgedResponse putResponse = client().admin()
                .indices()
                .preparePutMapping("test")
                .setType(MapperService.DEFAULT_MAPPING)
                .setSource(mappingBuilder)
                .get();
            assertThat(putResponse.isAcknowledged(), equalTo(true));
        }
        logger.info("Done Emptying _default_ mappings");

        {
            final GetMappingsResponse getResponse = client().admin()
                .indices()
                .prepareGetMappings("test")
                .addTypes(MapperService.DEFAULT_MAPPING)
                .get();
            final Map<String, Object> defaultMapping = getResponse.getMappings()
                .get("test")
                .get(MapperService.DEFAULT_MAPPING)
                .sourceAsMap();
            assertThat(defaultMapping, not(hasKey("date_detection")));
        }

        // now test you can change stuff that are normally unchangeable
        logger.info("Creating _default_ mappings with an analyzed field");
        try (XContentBuilder defaultMapping = JsonXContent.contentBuilder()) {

            defaultMapping.startObject();
            {
                defaultMapping.startObject(MapperService.DEFAULT_MAPPING);
                {
                    defaultMapping.startObject("properties");
                    {
                        defaultMapping.startObject("f");
                        {
                            defaultMapping.field("type", "text");
                            defaultMapping.field("index", true);
                        }
                        defaultMapping.endObject();
                    }
                    defaultMapping.endObject();
                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();

            final AcknowledgedResponse putResponse = client().admin()
                .indices()
                .preparePutMapping("test")
                .setType(MapperService.DEFAULT_MAPPING)
                .setSource(defaultMapping)
                .get();
            assertThat(putResponse.isAcknowledged(), equalTo(true));
        }

        logger.info("Changing _default_ mappings field from analyzed to non-analyzed");
        {
            try (XContentBuilder mappingBuilder = JsonXContent.contentBuilder()) {
                mappingBuilder.startObject();
                {
                    mappingBuilder.startObject(MapperService.DEFAULT_MAPPING);
                    {
                        mappingBuilder.startObject("properties");
                        {
                            mappingBuilder.startObject("f");
                            {
                                mappingBuilder.field("type", "keyword");
                            }
                            mappingBuilder.endObject();
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();

                final AcknowledgedResponse putResponse = client().admin()
                    .indices()
                    .preparePutMapping("test")
                    .setType(MapperService.DEFAULT_MAPPING)
                    .setSource(mappingBuilder)
                    .get();
                assertThat(putResponse.isAcknowledged(), equalTo(true));
            }
        }
        logger.info("Done changing _default_ mappings field from analyzed to non-analyzed");

        {
            final GetMappingsResponse getResponse = client().admin()
                .indices()
                .prepareGetMappings("test")
                .addTypes(MapperService.DEFAULT_MAPPING)
                .get();
            final Map<String, Object> defaultMapping = getResponse.getMappings()
                .get("test")
                .get(MapperService.DEFAULT_MAPPING)
                .sourceAsMap();
            final Map<String, Object> fieldSettings = (Map<String, Object>) ((Map) defaultMapping.get("properties")).get("f");
            assertThat(fieldSettings, hasEntry("type", "keyword"));
        }

        // but we still validate the _default_ type
        logger.info("Confirming _default_ mappings validation");
        try (XContentBuilder mappingBuilder = JsonXContent.contentBuilder()) {

            mappingBuilder.startObject();
            {
                mappingBuilder.startObject(MapperService.DEFAULT_MAPPING);
                {
                    mappingBuilder.startObject("properites");
                    {
                        mappingBuilder.startObject("f");
                        {
                            mappingBuilder.field("type", "non-existent");
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();

            expectThrows(
                MapperParsingException.class,
                () -> client().admin()
                    .indices()
                    .preparePutMapping("test")
                    .setType(MapperService.DEFAULT_MAPPING)
                    .setSource(mappingBuilder)
                    .get()
            );
        }

    }

}
