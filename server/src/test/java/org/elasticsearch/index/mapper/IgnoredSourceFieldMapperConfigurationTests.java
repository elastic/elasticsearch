/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class IgnoredSourceFieldMapperConfigurationTests extends MapperServiceTestCase {
    public void testDisableIgnoredSourceRead() throws IOException {
        var mapperService = mapperServiceWithCustomSettings(
            Map.of(IgnoredSourceFieldMapper.SKIP_IGNORED_SOURCE_READ_SETTING.getKey(), true),
            b -> {
                b.startObject("fallback_field");
                {
                    b.field("type", "long").field("doc_values", "false");
                }
                b.endObject();
                b.startObject("disabled_object");
                {
                    b.field("enabled", "false");
                    b.startObject("properties");
                    {
                        b.startObject("field").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
        );

        CheckedConsumer<XContentBuilder, IOException> inputDocument = b -> {
            b.field("fallback_field", 111);
            b.startObject("disabled_object");
            {
                b.field("field", "hey");
            }
            b.endObject();
        };

        var doc = mapperService.documentMapper().parse(source(inputDocument));
        // Field was written.
        assertNotNull(doc.docs().get(0).getField(IgnoredSourceFieldMapper.NAME));

        String syntheticSource = syntheticSource(mapperService.documentMapper(), inputDocument);
        // Values are not loaded.
        assertEquals("{}", syntheticSource);

        mapperService.getIndexSettings()
            .getScopedSettings()
            .applySettings(Settings.builder().put(IgnoredSourceFieldMapper.SKIP_IGNORED_SOURCE_READ_SETTING.getKey(), false).build());

        doc = mapperService.documentMapper().parse(source(inputDocument));
        // Field was written.
        assertNotNull(doc.docs().get(0).getField(IgnoredSourceFieldMapper.NAME));

        syntheticSource = syntheticSource(mapperService.documentMapper(), inputDocument);
        // Values are loaded.
        assertEquals("{\"disabled_object\":{\"field\":\"hey\"},\"fallback_field\":111}", syntheticSource);
    }

    public void testDisableIgnoredSourceWrite() throws IOException {
        var mapperService = mapperServiceWithCustomSettings(
            Map.of(IgnoredSourceFieldMapper.SKIP_IGNORED_SOURCE_WRITE_SETTING.getKey(), true),
            b -> {
                b.startObject("fallback_field");
                {
                    b.field("type", "long").field("doc_values", "false");
                }
                b.endObject();
                b.startObject("disabled_object");
                {
                    b.field("enabled", "false");
                    b.startObject("properties");
                    {
                        b.startObject("field").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
        );

        CheckedConsumer<XContentBuilder, IOException> inputDocument = b -> {
            b.field("fallback_field", 111);
            b.startObject("disabled_object");
            {
                b.field("field", "hey");
            }
            b.endObject();
        };

        var doc = mapperService.documentMapper().parse(source(inputDocument));
        // Field is not written.
        assertNull(doc.docs().get(0).getField(IgnoredSourceFieldMapper.NAME));

        String syntheticSource = syntheticSource(mapperService.documentMapper(), inputDocument);
        // Values are not loaded.
        assertEquals("{}", syntheticSource);

        mapperService.getIndexSettings()
            .getScopedSettings()
            .applySettings(Settings.builder().put(IgnoredSourceFieldMapper.SKIP_IGNORED_SOURCE_WRITE_SETTING.getKey(), false).build());

        doc = mapperService.documentMapper().parse(source(inputDocument));
        // Field was written.
        assertNotNull(doc.docs().get(0).getField(IgnoredSourceFieldMapper.NAME));

        syntheticSource = syntheticSource(mapperService.documentMapper(), inputDocument);
        // Values are loaded.
        assertEquals("{\"disabled_object\":{\"field\":\"hey\"},\"fallback_field\":111}", syntheticSource);
    }

    private MapperService mapperServiceWithCustomSettings(
        Map<String, Boolean> customSettings,
        CheckedConsumer<XContentBuilder, IOException> mapping
    ) throws IOException {
        var settings = Settings.builder();
        for (var entry : customSettings.entrySet()) {
            settings.put(entry.getKey(), entry.getValue());
        }
        settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC);
        return createMapperService(settings.build(), mapping(mapping));
    }

    protected void validateRoundTripReader(String syntheticSource, DirectoryReader reader, DirectoryReader roundTripReader)
        throws IOException {
        // Disabling this field via index settings leads to some values not being present in source and assertReaderEquals validation to
        // fail as a result.
        // This is expected, these settings are introduced only as a safety net when related logic blocks ingestion or search
        // and we would rather lose some part of source but unblock the workflow.
    }
}
