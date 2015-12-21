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
package org.elasticsearch.index.mapper.parent;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class ParentMappingTests extends ESSingleNodeTestCase {

    public void testParentSetInDocNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject().field("_parent", "1122").endObject().bytes()).type("type").id("1"));
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Field [_parent] is a metadata field and cannot be added inside a document"));
        }
    }

    public void testParentSetInDocBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "p_type").endObject()
                .endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", settings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("_parent", "1122")
                .field("x_field", "x_value")
                .endObject()
                .bytes()).type("type").id("1"));

        assertEquals("1122", doc.parent());
        assertEquals(Uid.createUid("p_type", "1122"), doc.rootDoc().get("_parent"));
    }

    public void testParentSet() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "p_type").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("x_field", "x_value")
                .endObject()
                .bytes()).type("type").id("1").parent("1122"));

        assertEquals(Uid.createUid("p_type", "1122"), doc.rootDoc().get("_parent"));
    }
}
