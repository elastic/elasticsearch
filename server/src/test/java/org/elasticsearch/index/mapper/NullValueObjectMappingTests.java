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

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class NullValueObjectMappingTests extends ESSingleNodeTestCase {
    public void testNullValueObject() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("obj1").field("type", "object").endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("obj1").endObject()
                        .field("value1", "test1")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("value1"), equalTo("test1"));

        doc = defaultMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("obj1")
                        .field("value1", "test1")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("value1"), equalTo("test1"));

        doc = defaultMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("obj1").field("field", "value").endObject()
                        .field("value1", "test1")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("obj1.field"), equalTo("value"));
        assertThat(doc.rootDoc().get("value1"), equalTo("test1"));
    }
}
