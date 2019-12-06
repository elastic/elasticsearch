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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.index.mapper.IdFieldMapper.ID_FIELD_DATA_DEPRECATION_MESSAGE;
import static org.hamcrest.Matchers.containsString;

public class IdFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("_id", "1").endObject()), XContentType.JSON));
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Field [_id] is a metadata field and cannot be added inside a document"));
        }
    }

    public void testDefaults() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE);
        ParsedDocument document = mapper.parse(new SourceToParse("index", "id",
            new BytesArray("{}"), XContentType.JSON));
        IndexableField[] fields = document.rootDoc().getFields(IdFieldMapper.NAME);
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.DOCS, fields[0].fieldType().indexOptions());
        assertTrue(fields[0].fieldType().stored());
        assertEquals(Uid.encodeId("id"), fields[0].binaryValue());
    }

    public void testEnableFieldData() throws IOException {
        IndexService service = createIndex("test", Settings.EMPTY);
        MapperService mapperService = service.mapperService();
        mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE);
        IdFieldMapper.IdFieldType ft = (IdFieldMapper.IdFieldType) service.mapperService().fullName("_id");

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> ft.fielddataBuilder("test").build(mapperService.getIndexSettings(),
                ft, null, null, mapperService));
        assertThat(exc.getMessage(), containsString(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()));

        client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey(), true))
            .get();
        try {
            ft.fielddataBuilder("test").build(mapperService.getIndexSettings(),
                ft, null, null, mapperService);
            assertWarnings(ID_FIELD_DATA_DEPRECATION_MESSAGE);
        } finally {
            // unset cluster setting
            client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()))
                .get();
        }
    }

}
