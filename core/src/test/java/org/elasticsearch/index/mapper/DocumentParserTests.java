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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

// TODO: make this a real unit test
public class DocumentParserTests extends ESSingleNodeTestCase {

    public void testTypeDisabled() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("enabled", false).endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject().startObject("foo")
            .field("field", "1234")
            .endObject().endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.rootDoc().getField(UidFieldMapper.NAME));
    }

    public void testFieldDisabled() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").field("enabled", false).endObject()
            .startObject("bar").field("type", "integer").endObject()
            .endObject().endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "1234")
            .field("bar", 10)
            .endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNull(doc.rootDoc().getField("foo"));
        assertNotNull(doc.rootDoc().getField("bar"));
        assertNotNull(doc.rootDoc().getField(UidFieldMapper.NAME));
    }
}
