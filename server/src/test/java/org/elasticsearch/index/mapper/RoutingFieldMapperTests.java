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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RoutingFieldMapperTests extends ESSingleNodeTestCase {

    public void testRoutingMapper() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "type", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()),
            XContentType.JSON, "routing_value"));

        assertThat(doc.rootDoc().get("_routing"), equalTo("routing_value"));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse(new SourceToParse("test", "type", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("_routing", "foo").endObject()),XContentType.JSON));
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), e.getMessage(),
                containsString("Field [_routing] is a metadata field and cannot be added inside a document"));
        }
    }
}
