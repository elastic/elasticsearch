/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
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
        LuceneDocument doc = parsedDoc.rootDoc();
        assertThat(doc.getFields("copy_test").size(), equalTo(2));
        assertThat(doc.getFields("copy_test").get(0).stringValue(), equalTo("foo"));
        assertThat(doc.getFields("copy_test").get(1).stringValue(), equalTo("bar"));

        assertThat(doc.getFields("another_field").size(), equalTo(2));
        assertThat(doc.getFields("another_field").get(0).stringValue(), equalTo("foo"));
        assertThat(doc.getFields("another_field").get(1).stringValue(), equalTo("42"));

        assertThat(doc.getFields("cyclic_test").size(), equalTo(2));
        assertThat(doc.getFields("cyclic_test").get(0).stringValue(), equalTo("foo"));
        assertThat(doc.getFields("cyclic_test").get(1).stringValue(), equalTo("bar"));

        assertThat(doc.getFields("int_to_str_test").size(), equalTo(1));
        assertThat(doc.getFields("int_to_str_test").get(0).fieldType().docValuesType(), equalTo(DocValuesType.NONE));
        assertThat(doc.getFields("int_to_str_test").get(0).numericValue().intValue(), equalTo(42));

        assertThat(doc.getFields("new_field").size(), equalTo(1));
        // new_field has doc values
        assertThat(doc.getFields("new_field").get(0).fieldType().docValuesType(), equalTo(DocValuesType.SORTED_NUMERIC));
        assertThat(doc.getFields("new_field").get(0).numericValue().intValue(), equalTo(42));

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

        LuceneDocument doc = docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.startObject("foo");
            {
                b.startObject("bar").field("baz", "zoo").endObject();
            }
            b.endObject();
        })).rootDoc();

        assertThat(doc.getFields("copy_test").size(), equalTo(1));
        assertThat(doc.getFields("copy_test").get(0).stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.inner.field").size(), equalTo(1));
        assertThat(doc.getFields("very.inner.field").get(0).stringValue(), equalTo("foo"));

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

        LuceneDocument doc = docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.field("new_field", "bar");
        })).rootDoc();

        assertThat(doc.getFields("copy_test").size(), equalTo(1));
        assertThat(doc.getFields("copy_test").get(0).stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.inner.field").size(), equalTo(1));
        assertThat(doc.getFields("very.inner.field").get(0).stringValue(), equalTo("foo"));

        assertThat(doc.getFields("new_field").size(), equalTo(1));
        assertThat(doc.getFields("new_field").get(0).stringValue(), equalTo("bar"));
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

        LuceneDocument doc = docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.field("new_field", "bar");
        })).rootDoc();

        assertThat(doc.getFields("copy_test").size(), equalTo(1));
        assertThat(doc.getFields("copy_test").get(0).stringValue(), equalTo("foo"));

        assertThat(doc.getFields("very.far.inner.field").size(), equalTo(1));
        assertThat(doc.getFields("very.far.inner.field").get(0).stringValue(), equalTo("foo"));

        assertThat(doc.getFields("new_field").size(), equalTo(1));
        assertThat(doc.getFields("new_field").get(0).stringValue(), equalTo("bar"));
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

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(source(b -> b.field("copy_test", "foo")))
        );

        assertThat(e.getMessage(), startsWith("[1:14] mapping set to strict, dynamic introduction of [very] within [_doc] is not allowed"));
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

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(source(b -> b.field("copy_test", "foo")))
        );

        assertThat(
            e.getMessage(),
            startsWith("[1:14] mapping set to strict, dynamic introduction of [field] within [very.far] is not allowed")
        );
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

        LuceneDocument nested = doc.docs().get(0);
        assertFieldValue(nested, "n1.n2.target", 3L);
        assertFieldValue(nested, "n1.target");
        assertFieldValue(nested, "target");
        assertEquals(1, nested.getFields(NestedPathFieldMapper.NAME).size());

        nested = doc.docs().get(1);
        assertFieldValue(nested, "n1.n2.target", 5L);
        assertFieldValue(nested, "n1.target");
        assertFieldValue(nested, "target");
        assertEquals(1, nested.getFields(NestedPathFieldMapper.NAME).size());

        nested = doc.docs().get(3);
        assertFieldValue(nested, "n1.n2.target", 7L);
        assertFieldValue(nested, "n1.target");
        assertFieldValue(nested, "target");
        assertEquals(1, nested.getFields(NestedPathFieldMapper.NAME).size());

        LuceneDocument parent = doc.docs().get(2);
        assertFieldValue(parent, "target");
        assertFieldValue(parent, "n1.target", 3L, 5L);
        assertFieldValue(parent, "n1.n2.target");
        assertEquals(1, parent.getFields(NestedPathFieldMapper.NAME).size());

        parent = doc.docs().get(4);
        assertFieldValue(parent, "target");
        assertFieldValue(parent, "n1.target", 7L);
        assertFieldValue(parent, "n1.n2.target");
        assertEquals(1, parent.getFields(NestedPathFieldMapper.NAME).size());

        LuceneDocument root = doc.docs().get(5);
        assertFieldValue(root, "target", 3L, 5L, 7L);
        assertFieldValue(root, "n1.target");
        assertFieldValue(root, "n1.n2.target");
        assertEquals(0, root.getFields(NestedPathFieldMapper.NAME).size());
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

        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> docMapper.parse(source(b -> {
            b.field("copy_test", "foo");
            b.field("new_field", "bar");
        })));

        assertThat(e.getMessage(), startsWith("[1:14] It is forbidden to create dynamic nested objects ([very]) through `copy_to`"));
    }

    private void assertFieldValue(LuceneDocument doc, String field, Number... expected) {
        List<IndexableField> values = doc.getFields(field);
        Number[] actual = new Number[values.size()];
        for (int i = 0; i < values.size(); ++i) {
            actual[i] = values.get(i).numericValue();
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
        Exception e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(fieldMapping(b -> {
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
        assertThat(e.getMessage(), Matchers.containsString("[copy_to] may not be used to copy from a multi-field: [field.bar]"));
    }

    public void testCopyToDateRangeFailure() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("date_copy")
                    .field("type", "date_range")
                    .endObject()
                    .startObject("date")
                    .field("type", "date_range")
                    .array("copy_to", "date_copy")
                    .endObject();
            }
            b.endObject();
        }));

        BytesReference json = BytesReference.bytes(
            jsonBuilder().startObject()
                .startObject("date")
                .field("gte", "2019-11-10T01:00:00.000Z")
                .field("lt", "2019-11-11T01:00:00.000Z")
                .endObject()
                .endObject()
        );

        DocumentParsingException ex = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc()
        );
        assertEquals(
            "[1:74] Cannot copy field [date] to fields [date_copy]. Copy-to currently only works for value-type fields, not objects.",
            ex.getMessage()
        );
    }

    public void testCopyToWithNullValue() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(
            topMapping(
                b -> b.startObject("properties")
                    .startObject("keyword_copy")
                    .field("type", "keyword")
                    .field("null_value", "default-value")
                    .endObject()
                    .startObject("keyword")
                    .field("type", "keyword")
                    .array("copy_to", "keyword_copy")
                    .endObject()
                    .endObject()
            )
        );

        BytesReference json = BytesReference.bytes(jsonBuilder().startObject().nullField("keyword").endObject());

        LuceneDocument document = docMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();
        assertEquals(0, document.getFields("keyword").size());

        List<IndexableField> fields = document.getFields("keyword_copy");
        assertEquals(1, fields.size());
    }

    public void testCopyToGeoPoint() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("geopoint_copy")
                    .field("type", "geo_point")
                    .endObject()
                    .startObject("geopoint")
                    .field("type", "geo_point")
                    .array("copy_to", "geopoint_copy")
                    .endObject();
            }
            b.endObject();
        }));
        // copy-to works for value-type representations
        {
            for (String value : List.of("41.12,-71.34", "drm3btev3e86", "POINT (-71.34 41.12)")) {
                BytesReference json = BytesReference.bytes(jsonBuilder().startObject().field("geopoint", value).endObject());

                LuceneDocument doc = docMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();

                List<IndexableField> fields = doc.getFields("geopoint");
                assertThat(fields.size(), equalTo(2));

                fields = doc.getFields("geopoint_copy");
                assertThat(fields.size(), equalTo(2));
            }
        }
        // check failure for object/array type representations
        {
            BytesReference json = BytesReference.bytes(
                jsonBuilder().startObject().startObject("geopoint").field("lat", 41.12).field("lon", -71.34).endObject().endObject()
            );

            DocumentParsingException ex = expectThrows(
                DocumentParsingException.class,
                () -> docMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc()
            );
            assertEquals(
                "[1:38] Cannot copy field [geopoint] to fields [geopoint_copy]. "
                    + "Copy-to currently only works for value-type fields, not objects.",
                ex.getMessage()
            );
        }
        {
            BytesReference json = BytesReference.bytes(
                jsonBuilder().startObject().array("geopoint", new double[] { -71.34, 41.12 }).endObject()
            );

            DocumentParsingException ex = expectThrows(
                DocumentParsingException.class,
                () -> docMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc()
            );
            assertEquals(
                "[1:26] Cannot copy field [geopoint] to fields [geopoint_copy]. "
                    + "Copy-to currently only works for value-type fields, not objects.",
                ex.getMessage()
            );
        }
    }

    public void testCopyToMultipleNested() throws IOException {

        // Don't copy values beyond a single step

        DocumentMapper documentMapper = createDocumentMapper("""
            { "_doc" : { "properties" : {
                "_all" : { "type" : "text" },
                "du" : {
                    "type" : "nested",
                    "include_in_root" : "true",
                    "properties" : {
                        "_all" : { "type" : "text" },
                        "bc" : {
                            "type" : "nested",
                            "include_in_parent" : "true",
                            "properties" : {
                                "_all" : { "type" : "text" },
                                "bc4" : {
                                    "type" : "nested",
                                    "include_in_parent" : "true",
                                    "properties" : {
                                        "_all" : { "type" : "text" },
                                        "area" : {
                                            "type" : "text",
                                            "copy_to" : [
                                                "_all",
                                                "du._all",
                                                "du.bc._all",
                                                "du.bc.bc4._all"
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }}}""");

        ParsedDocument doc = documentMapper.parse(source("""
            { "du" : { "bc" : [ { "bc4": { "area" : "foo" } }, { "bc4" : { "area" : "bar" } } ] } }
            """));

        assertEquals(1, doc.rootDoc().getFields("_id").size());
        assertEquals(2, doc.rootDoc().getFields("du._all").size());

    }
}
