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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
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
            .documentMapperWithAutoCreate().getDocumentMapper();

        ParsedDocument parsedDoc = documentMapper.parse(
            new SourceToParse("my-index", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("foo", 3)
                    .endObject()), XContentType.JSON));

        String expectedMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("foo").field("type", "long")
            .endObject().endObject().endObject().endObject());
        assertEquals(expectedMapping, parsedDoc.dynamicMappingsUpdate().toString());
    }

    public void testDynamicMappingSettingRemoval() {
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), false)
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> createIndex("test-index", settings));
        assertEquals(e.getMessage(), "Setting index.mapper.dynamic was removed after version 6.0.0");
        assertSettingDeprecationsAndWarnings(new Setting[] { MapperService.INDEX_MAPPER_DYNAMIC_SETTING });
    }

}
