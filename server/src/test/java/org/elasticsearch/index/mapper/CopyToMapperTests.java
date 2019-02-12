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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class CopyToMapperTests extends ESSingleNodeTestCase {
    @SuppressWarnings("unchecked")
    public void testCopyToFieldsParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("copy_test")
                .field("type", "text")
                .array("copy_to", "another_field", "cyclic_test")
                .endObject()

                .startObject("another_field")
                .field("type", "text")
                .endObject()

                .startObject("cyclic_test")
                .field("type", "text")
                .array("copy_to", "copy_test")
                .endObject()

                .startObject("int_to_str_test")
                .field("type", "integer")
                .field("doc_values", false)
                .array("copy_to",  "another_field", "new_field")
                .endObject()
                .endObject().endObject().endObject());

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type1").setSource(mapping, XContentType.JSON).get();
        DocumentMapper docMapper = index.mapperService().documentMapper("type1");
        Mapper fieldMapper = docMapper.mappers().getMapper("copy_test");

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

        // Check data parsing
        BytesReference json = BytesReference.bytes(jsonBuilder().startObject()
                .field("copy_test", "foo")
                .field("cyclic_test", "bar")
                .field("int_to_str_test", 42)
                .endObject());

        ParsedDocument parsedDoc = docMapper.parse(new SourceToParse("test", "type1", "1", json, XContentType.JSON));
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
        client().admin().indices().preparePutMapping("test").setType("type1")
            .setSource(parsedDoc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();

        docMapper = index.mapperService().documentMapper("type1");
        fieldMapper = docMapper.mappers().getMapper("new_field");
        assertThat(fieldMapper.typeName(), equalTo("long"));
    }

    public void testCopyToFieldsInnerObjectParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1").startObject("properties")

                .startObject("copy_test")
                .field("type", "text")
                .field("copy_to", "very.inner.field")
                .endObject()

                .startObject("very")
                .field("type", "object")
                .startObject("properties")
                .startObject("inner")
                .field("type", "object")
                .endObject()
                .endObject()
                .endObject()

                .endObject().endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        BytesReference json = BytesReference.bytes(jsonBuilder().startObject()
                .field("copy_test", "foo")
                .startObject("foo").startObject("bar").field("baz", "zoo").endObject().endObject()
                .endObject());

        ParseContext.Document doc = docMapper.parse(new SourceToParse("test", "type1", "1", json,
                XContentType.JSON)).rootDoc();
        assertThat(doc.getFields("copy_test").length, equalTo(1));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.inner.field").length, equalTo(1));
        assertThat(doc.getFields("very.inner.field")[0].stringValue(), equalTo("foo"));

    }

    public void testCopyToDynamicInnerObjectParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties")
                .startObject("copy_test")
                    .field("type", "text")
                    .field("copy_to", "very.inner.field")
                .endObject()
            .endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        BytesReference json = BytesReference.bytes(jsonBuilder().startObject()
                .field("copy_test", "foo")
                .field("new_field", "bar")
                .endObject());

        ParseContext.Document doc = docMapper.parse(new SourceToParse("test", "type1", "1", json,
                XContentType.JSON)).rootDoc();
        assertThat(doc.getFields("copy_test").length, equalTo(1));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.inner.field").length, equalTo(1));
        assertThat(doc.getFields("very.inner.field")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("new_field").length, equalTo(1));
        assertThat(doc.getFields("new_field")[0].stringValue(), equalTo("bar"));
    }

    public void testCopyToDynamicInnerInnerObjectParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties")
                .startObject("copy_test")
                    .field("type", "text")
                    .field("copy_to", "very.far.inner.field")
                .endObject()
                .startObject("very")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("far")
                            .field("type", "object")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        BytesReference json = BytesReference.bytes(jsonBuilder().startObject()
            .field("copy_test", "foo")
            .field("new_field", "bar")
            .endObject());

        ParseContext.Document doc = docMapper.parse(new SourceToParse("test", "type1", "1", json,
                XContentType.JSON)).rootDoc();
        assertThat(doc.getFields("copy_test").length, equalTo(1));
        assertThat(doc.getFields("copy_test")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.far.inner.field").length, equalTo(1));
        assertThat(doc.getFields("very.far.inner.field")[0].stringValue(), equalTo("foo"));

        assertThat(doc.getFields("new_field").length, equalTo(1));
        assertThat(doc.getFields("new_field")[0].stringValue(), equalTo("bar"));
    }

    public void testCopyToStrictDynamicInnerObjectParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .field("dynamic", "strict")
                .startObject("properties")
                    .startObject("copy_test")
                        .field("type", "text")
                        .field("copy_to", "very.inner.field")
                    .endObject()
                .endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        BytesReference json = BytesReference.bytes(jsonBuilder().startObject()
            .field("copy_test", "foo")
            .endObject());

        try {
            docMapper.parse(new SourceToParse("test", "type1", "1", json, XContentType.JSON)).rootDoc();
            fail();
        } catch (MapperParsingException ex) {
            assertThat(ex.getMessage(), startsWith("mapping set to strict, dynamic introduction of [very] within [type1] is not allowed"));
        }
    }

    public void testCopyToInnerStrictDynamicInnerObjectParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties")
                .startObject("copy_test")
                    .field("type", "text")
                    .field("copy_to", "very.far.field")
                .endObject()
                .startObject("very")
                    .field("type", "object")
                    .startObject("properties")
                        .startObject("far")
                            .field("type", "object")
                            .field("dynamic", "strict")
                        .endObject()
                    .endObject()
                .endObject()

            .endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        BytesReference json = BytesReference.bytes(jsonBuilder().startObject()
            .field("copy_test", "foo")
            .endObject());

        try {
            docMapper.parse(new SourceToParse("test", "type1", "1", json, XContentType.JSON)).rootDoc();
            fail();
        } catch (MapperParsingException ex) {
          assertThat(ex.getMessage(),
              startsWith("mapping set to strict, dynamic introduction of [field] within [very.far] is not allowed"));
        }
    }

    public void testCopyToFieldMerge() throws Exception {
        String mappingBefore = Strings.toString(jsonBuilder().startObject().startObject("type1").startObject("properties")

                .startObject("copy_test")
                .field("type", "text")
                .array("copy_to", "foo", "bar")
                .endObject()

                .endObject().endObject().endObject());

        String mappingAfter = Strings.toString(jsonBuilder().startObject().startObject("type1").startObject("properties")

                .startObject("copy_test")
                .field("type", "text")
                .array("copy_to", "baz", "bar")
                .endObject()

                .endObject().endObject().endObject());

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper docMapperBefore = mapperService.merge("type1", new CompressedXContent(mappingBefore),
            MapperService.MergeReason.MAPPING_UPDATE);
        FieldMapper fieldMapperBefore = (FieldMapper) docMapperBefore.mappers().getMapper("copy_test");

        assertEquals(Arrays.asList("foo", "bar"), fieldMapperBefore.copyTo().copyToFields());

        DocumentMapper docMapperAfter = mapperService.merge("type1", new CompressedXContent(mappingAfter),
            MapperService.MergeReason.MAPPING_UPDATE);
        FieldMapper fieldMapperAfter = (FieldMapper) docMapperAfter.mappers().getMapper("copy_test");

        assertEquals(Arrays.asList("baz", "bar"), fieldMapperAfter.copyTo().copyToFields());
        assertEquals(Arrays.asList("foo", "bar"), fieldMapperBefore.copyTo().copyToFields());
    }

    public void testCopyToNestedField() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("target")
                            .field("type", "long")
                            .field("doc_values", false)
                        .endObject()
                        .startObject("n1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("target")
                                    .field("type", "long")
                                    .field("doc_values", false)
                                .endObject()
                                .startObject("n2")
                                    .field("type", "nested")
                                    .startObject("properties")
                                        .startObject("target")
                                            .field("type", "long")
                                            .field("doc_values", false)
                                        .endObject()
                                        .startObject("source")
                                            .field("type", "long")
                                            .field("doc_values", false)
                                            .startArray("copy_to")
                                                .value("target") // should go to the root doc
                                                .value("n1.target") // should go to the parent doc
                                                .value("n1.n2.target") // should go to the current doc
                                            .endArray()
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();

            DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));

            XContentBuilder jsonDoc = XContentFactory.jsonBuilder()
                    .startObject()
                        .startArray("n1")
                            .startObject()
                                .startArray("n2")
                                    .startObject()
                                        .field("source", 3)
                                    .endObject()
                                    .startObject()
                                        .field("source", 5)
                                    .endObject()
                                .endArray()
                            .endObject()
                            .startObject()
                                .startArray("n2")
                                    .startObject()
                                        .field("source", 7)
                                    .endObject()
                                .endArray()
                            .endObject()
                        .endArray()
                    .endObject();

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(jsonDoc), XContentType.JSON));
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

    public void testCopyToChildNested() throws Exception {
        IndexService indexService = createIndex("test");
        XContentBuilder rootToNestedMapping = jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("source")
                            .field("type", "long")
                            .field("copy_to", "n1.target")
                        .endObject()
                        .startObject("n1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("target")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(BytesReference.bytes(rootToNestedMapping)),
                        MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), Matchers.startsWith("Illegal combination of [copy_to] and [nested] mappings"));

        XContentBuilder nestedToNestedMapping = jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("n1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("source")
                                    .field("type", "long")
                                    .field("copy_to", "n1.n2.target")
                                .endObject()
                                .startObject("n2")
                                    .field("type", "nested")
                                    .startObject("properties")
                                        .startObject("target")
                                            .field("type", "long")
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        e = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(BytesReference.bytes(nestedToNestedMapping)),
                        MergeReason.MAPPING_UPDATE));
    }

    public void testCopyToSiblingNested() throws Exception {
        IndexService indexService = createIndex("test");
        XContentBuilder rootToNestedMapping = jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("n1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("source")
                                    .field("type", "long")
                                    .field("copy_to", "n2.target")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("n2")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("target")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(BytesReference.bytes(rootToNestedMapping)),
                        MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), Matchers.startsWith("Illegal combination of [copy_to] and [nested] mappings"));
    }

    public void testCopyToObject() throws Exception {
        IndexService indexService = createIndex("test");
        XContentBuilder rootToNestedMapping = jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("source")
                            .field("type", "long")
                            .field("copy_to", "target")
                        .endObject()
                        .startObject("target")
                            .field("type", "object")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(BytesReference.bytes(rootToNestedMapping)),
                        MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), Matchers.startsWith("Cannot copy to field [target] since it is mapped as an object"));
    }

    public void testCopyToDynamicNestedObjectParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startArray("dynamic_templates")
                .startObject()
                    .startObject("objects")
                        .field("match_mapping_type", "object")
                        .startObject("mapping")
                            .field("type", "nested")
                        .endObject()
                    .endObject()
                .endObject()
            .endArray()
            .startObject("properties")
                .startObject("copy_test")
                    .field("type", "text")
                    .field("copy_to", "very.inner.field")
                .endObject()
            .endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        BytesReference json = BytesReference.bytes(jsonBuilder().startObject()
            .field("copy_test", "foo")
            .field("new_field", "bar")
            .endObject());

        try {
          docMapper.parse(new SourceToParse("test", "type1", "1", json, XContentType.JSON)).rootDoc();
          fail();
        } catch (MapperParsingException ex) {
            assertThat(ex.getMessage(), startsWith("It is forbidden to create dynamic nested objects ([very]) through `copy_to`"));
        }
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

    public void testCopyToMultiField() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("_doc")
                .startObject("properties")
                    .startObject("my_field")
                        .field("type", "keyword")
                        .field("copy_to", "my_field.bar")
                        .startObject("fields")
                            .startObject("bar")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject());

        MapperService mapperService = createIndex("test").mapperService();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE));
        assertEquals("[copy_to] may not be used to copy to a multi-field: [my_field.bar]", e.getMessage());
    }

    public void testNestedCopyTo() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("_doc")
                .startObject("properties")
                    .startObject("n")
                        .field("type", "nested")
                        .startObject("properties")
                            .startObject("foo")
                                .field("type", "keyword")
                                .field("copy_to", "n.bar")
                            .endObject()
                            .startObject("bar")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject());

        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE); // no exception
    }

    public void testNestedCopyToMultiField() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("_doc")
                .startObject("properties")
                    .startObject("n")
                        .field("type", "nested")
                        .startObject("properties")
                            .startObject("my_field")
                                .field("type", "keyword")
                                .field("copy_to", "n.my_field.bar")
                                .startObject("fields")
                                    .startObject("bar")
                                        .field("type", "text")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject());

        MapperService mapperService = createIndex("test").mapperService();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE));
        assertEquals("[copy_to] may not be used to copy to a multi-field: [n.my_field.bar]", e.getMessage());
    }

    public void testCopyFromMultiField() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("_doc")
                .startObject("properties")
                    .startObject("my_field")
                        .field("type", "keyword")
                        .startObject("fields")
                            .startObject("bar")
                                .field("type", "text")
                                .field("copy_to", "my_field.baz")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject());

        MapperService mapperService = createIndex("test").mapperService();
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapperService.merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(),
                Matchers.containsString("copy_to in multi fields is not allowed. Found the copy_to in field [bar] " +
                        "which is within a multi field."));
    }
}
