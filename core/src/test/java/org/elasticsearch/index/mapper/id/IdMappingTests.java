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

package org.elasticsearch.index.mapper.id;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IdMappingTests extends ESSingleNodeTestCase {
    
    public void testId() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), nullValue());

        try {
            docMapper.parse("test", "type", null, XContentFactory.jsonBuilder()
                    .startObject()
                    .endObject()
                    .bytes());
            fail("expect missing id");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("No id found"));
        }
    }
    
    public void testIdIndexedBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_id").field("index", "not_analyzed").endObject()
                .endObject().endObject().string();
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), notNullValue());
    }
    
    public void testIdPathBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_id").field("path", "my_path").endObject()
                .endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2_ID).build();
        DocumentMapper docMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        // serialize the id mapping
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder = docMapper.idFieldMapper().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String serialized_id_mapping = builder.string();

        String expected_id_mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("_id").field("path", "my_path").endObject()
                .endObject().string();

        assertThat(serialized_id_mapping, equalTo(expected_id_mapping));
    }

    public void testIncludeInObjectBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
            .startObject()
            .field("_id", "1")
            .endObject()
            .bytes()).type("type"));

        // _id is not indexed so we need to check _uid
        assertEquals(Uid.createUid("type", "1"), doc.rootDoc().get(UidFieldMapper.NAME));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject().field("_id", "1").endObject().bytes()).type("type"));
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Field [_id] is a metadata field and cannot be added inside a document"));
        }
    }
}
