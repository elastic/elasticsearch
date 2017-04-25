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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.instanceOf;

public class TypeFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaults() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        TypeFieldMapper typeMapper = docMapper.metadataMapper(TypeFieldMapper.class);
        assertFalse(typeMapper.enabled());
        assertEquals(mapping, docMapper.mappingSource().toString());
    }

    public void testOldDefaults() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_0_0).build();
        DocumentMapper docMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        TypeFieldMapper typeMapper = docMapper.metadataMapper(TypeFieldMapper.class);
        assertTrue(typeMapper.enabled());
        assertEquals(mapping, docMapper.mappingSource().toString());
    }

    public void testDocValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        TypeFieldMapper typeMapper = docMapper.metadataMapper(TypeFieldMapper.class);
        assertTrue(typeMapper.fieldType().hasDocValues());
        assertThat(typeMapper.fieldType().fielddataBuilder(), instanceOf(DocValuesIndexFieldData.Builder.class));
    }

    public void testEnabled() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_type")
                    .field("enabled", true)
                .endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        TypeFieldMapper typeMapper = docMapper.metadataMapper(TypeFieldMapper.class);
        assertTrue(typeMapper.enabled());
        assertEquals(mapping, docMapper.mappingSource().toString());
    }

    public void testEnableOnExistingMapping() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper docMapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, randomBoolean());
        TypeFieldMapper typeMapper = docMapper.metadataMapper(TypeFieldMapper.class);
        assertFalse(typeMapper.enabled());

        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_type")
                    .field("enabled", true)
                .endObject()
                .endObject().endObject().string();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("type", new CompressedXContent(enabledMapping), MergeReason.MAPPING_UPDATE, randomBoolean()));
        assertEquals("Cannot change value of the [enabled] attribute of the [_type] field from false to true", e.getMessage());
    }
}
