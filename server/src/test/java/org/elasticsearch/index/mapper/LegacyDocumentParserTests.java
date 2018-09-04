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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class LegacyDocumentParserTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testNestedHaveIdAndTypeFields() throws Exception {
        DocumentMapperParser mapperParser1 = createIndex("index1", Settings.builder()
                .put("index.version.created", Version.V_5_6_0) // allows for multiple types
                .build()
        ).mapperService().documentMapperParser();
        DocumentMapperParser mapperParser2 = createIndex("index2").mapperService().documentMapperParser();

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties");
        {
            mapping.startObject("foo");
            mapping.field("type", "nested");
            {
                mapping.startObject("properties");
                {

                    mapping.startObject("bar");
                    mapping.field("type", "keyword");
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        {
            mapping.startObject("baz");
            mapping.field("type", "keyword");
            mapping.endObject();
        }
        mapping.endObject().endObject().endObject();
        DocumentMapper mapper1 = mapperParser1.parse("type", new CompressedXContent(Strings.toString(mapping)));
        DocumentMapper mapper2 = mapperParser2.parse("type", new CompressedXContent(Strings.toString(mapping)));

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
        {
            doc.startArray("foo");
            {
                doc.startObject();
                doc.field("bar", "value1");
                doc.endObject();
            }
            doc.endArray();
            doc.field("baz", "value2");
        }
        doc.endObject();

        // Verify in the case where multiple types are allowed that the _uid field is added to nested documents:
        ParsedDocument result = mapper1.parse(SourceToParse.source("index1", "type", "1", BytesReference.bytes(doc), XContentType.JSON));
        assertEquals(2, result.docs().size());
        // Nested document:
        assertNull(result.docs().get(0).getField(IdFieldMapper.NAME));
        assertNotNull(result.docs().get(0).getField(UidFieldMapper.NAME));
        assertEquals("type#1", result.docs().get(0).getField(UidFieldMapper.NAME).stringValue());
        assertEquals(UidFieldMapper.Defaults.NESTED_FIELD_TYPE, result.docs().get(0).getField(UidFieldMapper.NAME).fieldType());
        assertNotNull(result.docs().get(0).getField(TypeFieldMapper.NAME));
        assertEquals("__foo", result.docs().get(0).getField(TypeFieldMapper.NAME).stringValue());
        assertEquals("value1", result.docs().get(0).getField("foo.bar").binaryValue().utf8ToString());
        // Root document:
        assertNull(result.docs().get(1).getField(IdFieldMapper.NAME));
        assertNotNull(result.docs().get(1).getField(UidFieldMapper.NAME));
        assertEquals("type#1", result.docs().get(1).getField(UidFieldMapper.NAME).stringValue());
        assertEquals(UidFieldMapper.Defaults.FIELD_TYPE, result.docs().get(1).getField(UidFieldMapper.NAME).fieldType());
        assertNotNull(result.docs().get(1).getField(TypeFieldMapper.NAME));
        assertEquals("type", result.docs().get(1).getField(TypeFieldMapper.NAME).stringValue());
        assertEquals("value2", result.docs().get(1).getField("baz").binaryValue().utf8ToString());

        // Verify in the case where only a single type is allowed that the _id field is added to nested documents:
        result = mapper2.parse(SourceToParse.source("index2", "type", "1", BytesReference.bytes(doc), XContentType.JSON));
        assertEquals(2, result.docs().size());
        // Nested document:
        assertNull(result.docs().get(0).getField(UidFieldMapper.NAME));
        assertNotNull(result.docs().get(0).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(0).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.NESTED_FIELD_TYPE, result.docs().get(0).getField(IdFieldMapper.NAME).fieldType());
        assertNotNull(result.docs().get(0).getField(TypeFieldMapper.NAME));
        assertEquals("__foo", result.docs().get(0).getField(TypeFieldMapper.NAME).stringValue());
        assertEquals("value1", result.docs().get(0).getField("foo.bar").binaryValue().utf8ToString());
        // Root document:
        assertNull(result.docs().get(1).getField(UidFieldMapper.NAME));
        assertNotNull(result.docs().get(1).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(1).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.FIELD_TYPE, result.docs().get(1).getField(IdFieldMapper.NAME).fieldType());
        assertNull(result.docs().get(1).getField(TypeFieldMapper.NAME));
        assertEquals("value2", result.docs().get(1).getField("baz").binaryValue().utf8ToString());
    }

}
