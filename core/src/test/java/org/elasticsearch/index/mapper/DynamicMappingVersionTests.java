/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

public class DynamicMappingVersionTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDynamicMappingDefault() throws IOException {
        MapperService mapperService = createIndex("my-index").mapperService();
        DocumentMapper documentMapper = mapperService
            .documentMapperWithAutoCreate("my-type").getDocumentMapper();

        ParsedDocument parsedDoc = documentMapper.parse(
            SourceToParse.source("my-index", "my-type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", 3)
                .endObject()
                .bytes(), XContentType.JSON));

        String expectedMapping = XContentFactory.jsonBuilder().startObject()
            .startObject("my-type")
            .startObject("properties")
            .startObject("foo").field("type", "long")
            .endObject().endObject().endObject().endObject().string();
        assertEquals(expectedMapping, parsedDoc.dynamicMappingsUpdate().toString());
    }

    public void testDynamicMappingDisablePreEs6() {
        Settings settingsPreEs6 = Settings.builder()
            .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), false)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_0_0)
            .build();
        MapperService preEs6MapperService = createIndex("pre-es6-index", settingsPreEs6).mapperService();
        Exception e = expectThrows(TypeMissingException.class,
            () -> preEs6MapperService.documentMapperWithAutoCreate("pre-es6-type"));
        assertEquals(e.getMessage(), "type[pre-es6-type] missing");
    }
}
