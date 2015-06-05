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

package org.elasticsearch.index.mapper.typelevels;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ParseDocumentTypeLevelsTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testNoLevel() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("test1", "value1")
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testTypeLevel() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                .field("test1", "value1")
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject().endObject()
                .bytes());

        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testNoLevelWithFieldTypeAsValue() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("type", "value_type")
                .field("test1", "value1")
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testTypeLevelWithFieldTypeAsValue() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                .field("type", "value_type")
                .field("test1", "value1")
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject().endObject()
                .bytes());

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testNoLevelWithFieldTypeAsObject() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type").field("type_field", "type_value").endObject()
                .field("test1", "value1")
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject()
                .bytes());

        // in this case, we analyze the type object as the actual document, and ignore the other same level fields
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
    }

    @Test
    public void testTypeLevelWithFieldTypeAsObject() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                .startObject("type").field("type_field", "type_value").endObject()
                .field("test1", "value1")
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject().endObject()
                .bytes());

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testNoLevelWithFieldTypeAsValueNotFirst() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                .field("test1", "value1")
                .field("test2", "value2")
                .field("type", "value_type")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject().endObject()
                .bytes());

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testTypeLevelWithFieldTypeAsValueNotFirst() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                .field("test1", "value1")
                .field("type", "value_type")
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject().endObject()
                .bytes());

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testNoLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("test1", "value1")
                .startObject("type").field("type_field", "type_value").endObject()
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject()
                .bytes());

        // when the type is not the first one, we don't confuse it...
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    @Test
    public void testTypeLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(defaultMapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                .field("test1", "value1")
                .startObject("type").field("type_field", "type_value").endObject()
                .field("test2", "value2")
                .startObject("inner").field("inner_field", "inner_value").endObject()
                .endObject().endObject()
                .bytes());

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }
}
