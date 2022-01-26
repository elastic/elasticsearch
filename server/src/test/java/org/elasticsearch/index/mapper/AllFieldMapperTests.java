/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentFactory;

import static org.hamcrest.CoreMatchers.containsString;

public class AllFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testAllDisabled() throws Exception {
        {
            final Version version = VersionUtils.randomVersionBetween(
                random(),
                Version.V_6_0_0,
                Version.V_7_0_0.minimumCompatibilityVersion()
            );
            IndexService indexService = createIndex(
                "test_6x",
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build()
            );
            String mappingDisabled = Strings.toString(
                XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().endObject()
            );
            indexService.mapperService().merge("_doc", new CompressedXContent(mappingDisabled), MergeReason.MAPPING_UPDATE);
            assertEquals(
                "{\"_doc\":{\"_all\":{\"enabled\":false}}}",
                Strings.toString(indexService.mapperService().documentMapper().mapping())
            );

            String mappingEnabled = Strings.toString(
                XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true).endObject().endObject()
            );
            MapperParsingException exc = expectThrows(
                MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingEnabled), MergeReason.MAPPING_UPDATE)
            );
            assertThat(exc.getMessage(), containsString("[_all] is disabled in this version."));
        }
        {
            IndexService indexService = createIndex("test");
            String mappingEnabled = Strings.toString(
                XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true).endObject().endObject()
            );
            MapperParsingException exc = expectThrows(
                MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingEnabled), MergeReason.MAPPING_UPDATE)
            );
            assertThat(exc.getMessage(), containsString("unsupported parameters:  [_all"));

            String mappingDisabled = Strings.toString(
                XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().endObject()
            );
            exc = expectThrows(
                MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingDisabled), MergeReason.MAPPING_UPDATE)
            );
            assertThat(exc.getMessage(), containsString("unsupported parameters:  [_all"));

            String mappingAll = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_all").endObject().endObject());
            exc = expectThrows(
                MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingAll), MergeReason.MAPPING_UPDATE)
            );
            assertThat(exc.getMessage(), containsString("unsupported parameters:  [_all"));

            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().endObject());
            indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
            assertEquals("{\"_doc\":{}}", indexService.mapperService().documentMapper().mapping().toString());
        }
    }

    public void testUpdateDefaultSearchAnalyzer() throws Exception {
        IndexService indexService = createIndex(
            "test",
            Settings.builder()
                .put("index.analysis.analyzer.default_search.type", "custom")
                .put("index.analysis.analyzer.default_search.tokenizer", "standard")
                .build()
        );
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, indexService.mapperService().documentMapper().mapping().toString());
    }

}
