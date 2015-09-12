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

package org.elasticsearch.index.mapper.nested;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper.Dynamic;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class NestedMappingTests extends ESSingleNodeTestCase {

    @Test
    public void emptyNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .nullField("nested1")
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(1));

        doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested").endArray()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(1));
    }

    @Test
    public void singleNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startObject("nested1").field("field1", "1").field("field2", "2").endObject()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(2));
        assertThat(doc.docs().get(0).get(TypeFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePathAsString()));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("2"));

        assertThat(doc.docs().get(1).get("field"), equalTo("value"));


        doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").field("field2", "2").endObject()
                .startObject().field("field1", "3").field("field2", "4").endObject()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(3));
        assertThat(doc.docs().get(0).get(TypeFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePathAsString()));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("3"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("4"));
        assertThat(doc.docs().get(1).get(TypeFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePathAsString()));
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(1).get("nested1.field2"), equalTo("2"));

        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }

    @Test
    public void multiNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested")
                .endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").startArray("nested2").startObject().field("field2", "2").endObject().startObject().field("field2", "3").endObject().endArray().endObject()
                .startObject().field("field1", "4").startArray("nested2").startObject().field("field2", "5").endObject().startObject().field("field2", "6").endObject().endArray().endObject()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).get("nested1.nested2.field2"), nullValue());
    }

    @Test
    public void multiObjectAndNested1() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_parent", true)
                .endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").startArray("nested2").startObject().field("field2", "2").endObject().startObject().field("field2", "3").endObject().endArray().endObject()
                .startObject().field("field1", "4").startArray("nested2").startObject().field("field2", "5").endObject().startObject().field("field2", "6").endObject().endArray().endObject()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).get("nested1.nested2.field2"), nullValue());
    }

    @Test
    public void multiObjectAndNested2() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").field("include_in_parent", true).startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_parent", true)
                .endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").startArray("nested2").startObject().field("field2", "2").endObject().startObject().field("field2", "3").endObject().endArray().endObject()
                .startObject().field("field1", "4").startArray("nested2").startObject().field("field2", "5").endObject().startObject().field("field2", "6").endObject().endArray().endObject()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).getFields("nested1.field1").length, equalTo(2));
        assertThat(doc.docs().get(6).getFields("nested1.nested2.field2").length, equalTo(4));
    }

    @Test
    public void multiRootAndNested1() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_root", true)
                .endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.nested().isIncludeInRoot(), equalTo(false));
        ObjectMapper nested2Mapper = docMapper.objectMappers().get("nested1.nested2");
        assertThat(nested2Mapper.nested().isNested(), equalTo(true));
        assertThat(nested2Mapper.nested().isIncludeInParent(), equalTo(false));
        assertThat(nested2Mapper.nested().isIncludeInRoot(), equalTo(true));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").startArray("nested2").startObject().field("field2", "2").endObject().startObject().field("field2", "3").endObject().endArray().endObject()
                .startObject().field("field1", "4").startArray("nested2").startObject().field("field2", "5").endObject().startObject().field("field2", "6").endObject().endArray().endObject()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).getFields("nested1.nested2.field2").length, equalTo(4));
    }

    @Test
    public void nestedArray_strict() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").field("dynamic", "strict").startObject("properties")
                .startObject("field1").field("type", "string")
                .endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.hasNestedObjects(), equalTo(true));
        ObjectMapper nested1Mapper = docMapper.objectMappers().get("nested1");
        assertThat(nested1Mapper.nested().isNested(), equalTo(true));
        assertThat(nested1Mapper.dynamic(), equalTo(Dynamic.STRICT));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").endObject()
                .startObject().field("field1", "4").endObject()
                .endArray()
                .endObject()
                .bytes());

        assertThat(doc.docs().size(), equalTo(3));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }
}