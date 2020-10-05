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

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class CopyToMapperTests extends MapperServiceTestCase {

    @SuppressWarnings("unchecked")
    public void testCopyToFieldsParsing() throws Exception {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("copy_test");
            {
                b.field("type", "text");
                b.array("copy_to", "another_field", "cyclic_test");
            }
            b.endObject();
            b.startObject("another_field").field("type", "text").endObject();
            b.startObject("cyclic_test");
            {
                b.field("type", "text");
                b.array("copy_to", "copy_test");
            }
            b.endObject();
            b.startObject("int_to_str_test");
            {
                b.field("type", "integer");
                b.field("doc_values", false);
                b.array("copy_to", "another_field", "new_field");
            }
            b.endObject();
        }));
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("copy_test");

        // Check json serialization
        TextFieldMapper stringFieldMapper = (TextFieldMapper) fieldMapper;
        XContentBuilder builder = jsonBuilder().startObject();
        stringFieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        builder.close();
        Map<String, Object> serializedMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            serializedMap = parser.map();
        }
        Map<String, Object> copyTestMap = (Map<String, Object>) serializedMap.get("copy_test");
        assertThat(copyTestMap.get("type").toString(), is("text"));
        List<String> copyToList = (List<String>) copyTestMap.get("copy_to");
        assertThat(copyToList.size(), equalTo(2));
        assertThat(copyToList.get(0), equalTo("another_field"));
        assertThat(copyToList.get(1), equalTo("cyclic_test"));

        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(b -> {
            b.field("copy_test", "foo");
            b.field("cyclic_test", "bar");
            b.field("int_to_str_test", 42);
        }));
        ParseContext.Document doc = parsedDoc.rootDoc();
        assertThat(doc.getFields("copy_test").length, equalTo(2));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));
        assertThat(doc.getFields("copy_test")[1].stringValue(), equalTo("bar"));

        assertThat(doc.getFields("another_field").length, equalTo(2));
        assertThat(doc.getFields("another_field")[0].stringValue(), equalTo("foo"));
        assertThat(doc.getFields("another_field")[1].stringValue(), equalTo("42"));

        assertThat(doc.getFields("cyclic_test").length, equalTo(2));
        assertThat(doc.getFields("cyclic_test")[0].stringValue(), equalTo("foo"));
        assertThat(doc.getFields("cyclic_test")[1].stringValue(), equalTo("bar"));

        assertThat(doc.getFields("int_to_str_test").length, equalTo(1));
        assertThat(doc.getFields("int_to_str_test")[0].numericValue().intValue(), equalTo(42));

        assertThat(doc.getFields("new_field").length, equalTo(2)); // new field has doc values
        assertThat(doc.getFields("new_field")[0].numericValue().intValue(), equalTo(42));

        assertNotNull(parsedDoc.dynamicMappingsUpdate());

        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("new_field");
        assertThat(fieldMapper.typeName(), equalTo("long"));
    }

    public void testCopyToFieldsInnerObjectParsing() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("copy_test");
            {
                b.field("type", "text");
                b.field("copy_to", "very.inner.field");
            }
            b.endObject();
            b.startObject("very");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("inner").field("type", "object").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParseContext.Document doc = docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.startObject("foo");
            {
                b.startObject("bar").field("baz", "zoo").endObject();
            }
            b.endObject();
        })).rootDoc();

        assertThat(doc.getFields("copy_test").length, equalTo(1));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.inner.field").length, equalTo(1));
        assertThat(doc.getFields("very.inner.field")[0].stringValue(), equalTo("foo"));

    }

    public void testCopyToDynamicInnerObjectParsing() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("copy_test");
            {
                b.field("type", "text");
                b.field("copy_to", "very.inner.field");
            }
            b.endObject();
        }));

        ParseContext.Document doc = docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.field("new_field", "bar");
        })).rootDoc();

        assertThat(doc.getFields("copy_test").length, equalTo(1));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.inner.field").length, equalTo(1));
        assertThat(doc.getFields("very.inner.field")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("new_field").length, equalTo(1));
        assertThat(doc.getFields("new_field")[0].stringValue(), equalTo("bar"));
    }

    public void testCopyToDynamicInnerInnerObjectParsing() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("copy_test");
            {
                b.field("type", "text");
                b.field("copy_to", "very.far.inner.field");
            }
            b.endObject();
            b.startObject("very");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("far").field("type", "object").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParseContext.Document doc = docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.field("new_field", "bar");
        })).rootDoc();

        assertThat(doc.getFields("copy_test").length, equalTo(1));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.far.inner.field").length, equalTo(1));
        assertThat(doc.getFields("very.far.inner.field")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("new_field").length, equalTo(1));
        assertThat(doc.getFields("new_field")[0].stringValue(), equalTo("bar"));
    }

    public void testCopyToStrictDynamicInnerObjectParsing() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "strict");
            b.startObject("properties");
            {
                b.startObject("copy_test");
                {
                    b.field("type", "text");
                    b.field("copy_to", "very.inner.field");
                }
                b.endObject();
            }
            b.endObject();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(source(b -> b.field("copy_test", "foo"))));

        assertThat(e.getMessage(), startsWith("mapping set to strict, dynamic introduction of [very] within [_doc] is not allowed"));
    }

    public void testCopyToInnerStrictDynamicInnerObjectParsing() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("copy_test");
            {
                b.field("type", "text");
                b.field("copy_to", "very.far.field");
            }
            b.endObject();
            b.startObject("very");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("far");
                    {
                        b.field("type", "object");
                        b.field("dynamic", "strict");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(source(b -> b.field("copy_test", "foo"))));

        assertThat(e.getMessage(),
            startsWith("mapping set to strict, dynamic introduction of [field] within [very.far] is not allowed"));
    }

    public void testCopyToFieldMerge() throws Exception {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("copy_test");
            {
                b.field("type", "text");
                b.array("copy_to", "foo", "bar");
            }
            b.endObject();
        }));
        DocumentMapper docMapperBefore = mapperService.documentMapper();
        FieldMapper fieldMapperBefore = (FieldMapper) docMapperBefore.mappers().getMapper("copy_test");

        assertEquals(Arrays.asList("foo", "bar"), fieldMapperBefore.copyTo().copyToFields());

        merge(mapperService, mapping(b -> {
            b.startObject("copy_test");
            {
                b.field("type", "text");
                b.array("copy_to", "baz", "bar");
            }
            b.endObject();
        }));

        DocumentMapper docMapperAfter = mapperService.documentMapper();
        FieldMapper fieldMapperAfter = (FieldMapper) docMapperAfter.mappers().getMapper("copy_test");

        assertEquals(Arrays.asList("baz", "bar"), fieldMapperAfter.copyTo().copyToFields());
        assertEquals(Arrays.asList("foo", "bar"), fieldMapperBefore.copyTo().copyToFields());
    }

    public void testCopyToNestedField() throws Exception {

            DocumentMapper mapper = createDocumentMapper(mapping(b -> {
                b.startObject("target");
                {
                    b.field("type", "long");
                    b.field("doc_values", false);
                }
                b.endObject();
                b.startObject("n1");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("target");
                        {
                            b.field("type", "long");
                            b.field("doc_values", false);
                        }
                        b.endObject();
                        b.startObject("n2");
                        {
                            b.field("type", "nested");
                            b.startObject("properties");
                            {
                                b.startObject("target");
                                {
                                    b.field("type", "long");
                                    b.field("doc_values", false);
                                }
                                b.endObject();
                                b.startObject("source");
                                {
                                    b.field("type", "long");
                                    b.field("doc_values", false);
                                    b.startArray("copy_to");
                                    {
                                        b.value("target"); // should go to the root doc
                                        b.value("n1.target"); // should go to the parent doc
                                        b.value("n1.n2.target"); // should go to the current doc
                                    }
                                    b.endArray();
                                }
                                b.endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("n1");
            {
                b.startObject();
                {
                    b.startArray("n2");
                    {
                        b.startObject().field("source", 3).endObject();
                        b.startObject().field("source", 5).endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.startArray("n2");
                    {
                        b.startObject().field("source", 7).endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        }));

        assertEquals(6, doc.docs().size());

        Document nested = doc.docs().get(0);
        assertFieldValue(nested, "n1.n2.target", 3L);
        assertFieldValue(nested, "n1.target");
        assertFieldValue(nested, "target");

        nested = doc.docs().get(1);
        assertFieldValue(nested, "n1.n2.target", 5L);
        assertFieldValue(nested, "n1.target");
        assertFieldValue(nested, "target");

        nested = doc.docs().get(3);
        assertFieldValue(nested, "n1.n2.target", 7L);
        assertFieldValue(nested, "n1.target");
        assertFieldValue(nested, "target");

        Document parent = doc.docs().get(2);
        assertFieldValue(parent, "target");
        assertFieldValue(parent, "n1.target", 3L, 5L);
        assertFieldValue(parent, "n1.n2.target");

        parent = doc.docs().get(4);
        assertFieldValue(parent, "target");
        assertFieldValue(parent, "n1.target", 7L);
        assertFieldValue(parent, "n1.n2.target");

        Document root = doc.docs().get(5);
        assertFieldValue(root, "target", 3L, 5L, 7L);
        assertFieldValue(root, "n1.target");
        assertFieldValue(root, "n1.n2.target");
    }

    public void testCopyToChildNested() {

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("source");
            {
                b.field("type", "long");
                b.field("copy_to", "n1.target");
            }
            b.endObject();
            b.startObject("n1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("target").field("type", "long").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), Matchers.startsWith("Illegal combination of [copy_to] and [nested] mappings"));

        expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("n1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("source");
                    {
                        b.field("type", "long");
                        b.field("copy_to", "n1.n2.target");
                    }
                    b.endObject();
                    b.startObject("n2");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        {
                            b.startObject("target").field("type", "long").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
    }

    public void testCopyToSiblingNested() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("n1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("source");
                    {
                        b.field("type", "long");
                        b.field("copy_to", "n2.target");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("n2");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("target").field("type", "long").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), Matchers.startsWith("Illegal combination of [copy_to] and [nested] mappings"));
    }

    public void testCopyToObject() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("source");
            {
                b.field("type", "long");
                b.field("copy_to", "target");
            }
            b.endObject();
            b.startObject("target");
            {
                b.field("type", "object");
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), Matchers.startsWith("Cannot copy to field [target] since it is mapped as an object"));
    }

    public void testCopyToDynamicNestedObjectParsing() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("objects");
                    {
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "nested").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
            b.startObject("properties");
            {
                b.startObject("copy_test");
                {
                    b.field("type", "text");
                    b.field("copy_to", "very.inner.field");
                }
                b.endObject();
            }
            b.endObject();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.field("new_field", "bar");
        })));

        assertThat(e.getMessage(), startsWith("It is forbidden to create dynamic nested objects ([very]) through `copy_to`"));
    }

    private void assertFieldValue(Document doc, String field, Number... expected) {
        IndexableField[] values = doc.getFields(field);
        if (values == null) {
            values = new IndexableField[0];
        }
        Number[] actual = new Number[values.length];
        for (int i = 0; i < values.length; ++i) {
            actual[i] = values[i].numericValue();
        }
        assertArrayEquals(expected, actual);
    }

    public void testCopyToMultiField() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("my_field");
            {
                b.field("type", "keyword");
                b.field("copy_to", "my_field.bar");
                b.startObject("fields");
                {
                    b.startObject("bar").field("type", "text").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertEquals("[copy_to] may not be used to copy to a multi-field: [my_field.bar]", e.getMessage());
    }

    public void testNestedCopyTo() throws IOException {
        createDocumentMapper(fieldMapping(b -> {
            b.field("type", "nested");
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "keyword");
                    b.field("copy_to", "n.bar");
                }
                b.endObject();
                b.startObject("bar").field("type", "text").endObject();
            }
            b.endObject();
        }));
    }

    public void testNestedCopyToMultiField() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "nested");
            b.startObject("properties");
            {
                b.startObject("my_field");
                {
                    b.field("type", "keyword");
                    b.field("copy_to", "field.my_field.bar");
                    b.startObject("fields");
                    {
                        b.startObject("bar").field("type", "text").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertEquals("[copy_to] may not be used to copy to a multi-field: [field.my_field.bar]", e.getMessage());
    }

    public void testCopyFromMultiField() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            {
                b.startObject("bar");
                {
                    b.field("type", "text");
                    b.field("copy_to", "my_field.baz");
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(e.getMessage(),
            Matchers.containsString("copy_to in multi fields is not allowed. Found the copy_to in field [bar] " +
                "which is within a multi field."));
    }
}
