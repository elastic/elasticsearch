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

package org.elasticsearch.index.mapper.compound;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class CompoundTypesTests extends ESSingleNodeTestCase {

    @Test
    public void testStringType() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat((double) doc.rootDoc().getField("field1").boost(), closeTo(1.0d, 0.000001d));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("field1").field("value", "value1").field("boost", 2.0f).endObject()
                .field("field2", "value2")
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat((double) doc.rootDoc().getField("field1").boost(), closeTo(2.0d, 0.000001d));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat((double) doc.rootDoc().getField("field1").boost(), closeTo(1.0d, 0.000001d));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));
    }
}