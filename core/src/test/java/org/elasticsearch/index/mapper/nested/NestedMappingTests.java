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

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper.Dynamic;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class NestedMappingTests extends ESSingleNodeTestCase {
    public void testEmptyNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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

    public void testSingleNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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

    public void testMultiNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested")
                .endObject().endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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

    public void testMultiObjectAndNested1() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_parent", true)
                .endObject().endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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

    public void testMultiObjectAndNested2() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").field("include_in_parent", true).startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_parent", true)
                .endObject().endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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

    public void testMultiRootAndNested1() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").startObject("properties")
                .startObject("nested2").field("type", "nested").field("include_in_root", true)
                .endObject().endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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

    public void testNestedArrayStrict() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("nested1").field("type", "nested").field("dynamic", "strict").startObject("properties")
                .startObject("field1").field("type", "text")
                .endObject().endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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

    public void testLimitOfNestedFieldsPerIndex() throws Exception {
        Function<String, String> mapping = type -> {
            try {
                return XContentFactory.jsonBuilder().startObject().startObject(type).startObject("properties")
                    .startObject("nested1").field("type", "nested").startObject("properties")
                    .startObject("nested2").field("type", "nested")
                    .endObject().endObject().endObject()
                    .endObject().endObject().endObject().string();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        // default limit allows at least two nested fields
        createIndex("test1").mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE, false);

        // explicitly setting limit to 0 prevents nested fields
        try {
            createIndex("test2", Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 0).build())
                .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE, false);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Limit of nested fields [0] in index [test2] has been exceeded"));
        }

        // setting limit to 1 with 2 nested fields fails
        try {
            createIndex("test3", Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 1).build())
                .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE, false);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Limit of nested fields [1] in index [test3] has been exceeded"));
        }

        MapperService mapperService = createIndex("test4", Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 2)
            .build()).mapperService();
        mapperService.merge("type1", new CompressedXContent(mapping.apply("type1")), MergeReason.MAPPING_UPDATE, false);
        // merging same fields, but different type is ok
        mapperService.merge("type2", new CompressedXContent(mapping.apply("type2")), MergeReason.MAPPING_UPDATE, false);
        // adding new fields from different type is not ok
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type3").startObject("properties").startObject("nested3")
            .field("type", "nested").startObject("properties").endObject().endObject().endObject().endObject().endObject().string();
        try {
            mapperService.merge("type3", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Limit of nested fields [2] in index [test4] has been exceeded"));
        }

        // do not check nested fields limit if mapping is not updated
        createIndex("test5", Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 0).build())
            .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_RECOVERY, false);
    }
}
