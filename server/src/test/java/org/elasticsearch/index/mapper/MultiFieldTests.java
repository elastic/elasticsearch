/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class MultiFieldTests extends MapperServiceTestCase {

    public void testMultiFieldMultiFields() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-multi-fields.json");
        MapperService mapperService = createMapperService(mapping);

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        LuceneDocument doc = mapperService.documentMapper().parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());

        f = doc.getField("name.indexed");
        assertThat(f.name(), equalTo("name.indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());

        f = doc.getField("name.not_indexed");
        assertThat(f.name(), equalTo("name.not_indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertEquals(IndexOptions.NONE, f.fieldType().indexOptions());

        f = doc.getField("object1.multi1");
        assertThat(f.name(), equalTo("object1.multi1"));

        f = doc.getField("object1.multi1.string");
        assertThat(f.name(), equalTo("object1.multi1.string"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("2010-01-01")));

        assertThat(mapperService.mappedField("name"), notNullValue());
        assertThat(mapperService.mappedField("name").type(), instanceOf(TextFieldType.class));
        assertTrue(mapperService.mappedField("name").isIndexed());
        assertTrue(mapperService.mappedField("name").isSearchable());
        assertTrue(mapperService.mappedField("name").isStored());
        assertTrue(mapperService.mappedField("name").getTextSearchInfo().isTokenized());

        assertThat(mapperService.mappedField("name.indexed"), notNullValue());
        assertThat(mapperService.mappedField("name").type(), instanceOf(TextFieldType.class));
        assertTrue(mapperService.mappedField("name.indexed").isIndexed());
        assertTrue(mapperService.mappedField("name.indexed").isSearchable());
        assertFalse(mapperService.mappedField("name.indexed").isStored());
        assertTrue(mapperService.mappedField("name.indexed").getTextSearchInfo().isTokenized());

        assertThat(mapperService.mappedField("name.not_indexed"), notNullValue());
        assertThat(mapperService.mappedField("name").type(), instanceOf(TextFieldType.class));
        assertFalse(mapperService.mappedField("name.not_indexed").isIndexed());
        assertFalse(mapperService.mappedField("name.not_indexed").isSearchable());
        assertTrue(mapperService.mappedField("name.not_indexed").isStored());
        assertTrue(mapperService.mappedField("name.not_indexed").getTextSearchInfo().isTokenized());

        assertThat(mapperService.mappedField("name.test1"), notNullValue());
        assertThat(mapperService.mappedField("name").type(), instanceOf(TextFieldType.class));
        assertTrue(mapperService.mappedField("name.test1").isIndexed());
        assertTrue(mapperService.mappedField("name.test1").isSearchable());
        assertTrue(mapperService.mappedField("name.test1").isStored());
        assertTrue(mapperService.mappedField("name.test1").getTextSearchInfo().isTokenized());
        assertThat(mapperService.mappedField("name.test1").eagerGlobalOrdinals(), equalTo(true));

        assertThat(mapperService.mappedField("object1.multi1"), notNullValue());
        assertThat(mapperService.mappedField("object1.multi1").type(), instanceOf(DateFieldMapper.DateFieldType.class));
        assertThat(mapperService.mappedField("object1.multi1.string"), notNullValue());
        assertThat(mapperService.mappedField("object1.multi1.string").type(), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertTrue(mapperService.mappedField("object1.multi1.string").isIndexed());
        assertTrue(mapperService.mappedField("object1.multi1.string").isSearchable());
        assertNotNull(mapperService.mappedField("object1.multi1.string").getTextSearchInfo());
        assertFalse(mapperService.mappedField("object1.multi1.string").getTextSearchInfo().isTokenized());
    }

    public void testBuildThenParse() throws Exception {
        DocumentMapper builderDocMapper = createDocumentMapper(mapping(b -> {
            b.startObject("name");
            b.field("type", "text");
            b.field("store", true);
            b.startObject("fields");
            {
                b.startObject("indexed").field("type", "text").endObject();
                b.startObject("not_indexed").field("type", "text").field("index", false).field("store", true).endObject();
            }
            b.endObject();
            b.endObject();
        }));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        LuceneDocument doc = builderDocMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());

        f = doc.getField("name.indexed");
        assertThat(f.name(), equalTo("name.indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().tokenized(), equalTo(true));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());

        f = doc.getField("name.not_indexed");
        assertThat(f.name(), equalTo("name.not_indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertEquals(IndexOptions.NONE, f.fieldType().indexOptions());
    }

    // The underlying order of the fields in multi fields in the mapping source should always be consistent, if not this
    // can to unnecessary re-syncing of the mappings between the local instance and cluster state
    public void testMultiFieldsInConsistentOrder() throws Exception {
        String[] multiFieldNames = new String[randomIntBetween(2, 10)];
        Set<String> seenFields = new HashSet<>();
        for (int i = 0; i < multiFieldNames.length; i++) {
            multiFieldNames[i] = randomValueOtherThanMany(s -> seenFields.add(s) == false, () -> randomAlphaOfLength(4));
        }

        DocumentMapper docMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            for (String multiFieldName : multiFieldNames) {
                b.startObject(multiFieldName).field("type", "text").endObject();
            }
            b.endObject();
        }));
        Arrays.sort(multiFieldNames);

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(
            docMapper.mappingSource().compressedReference(),
            true,
            XContentType.JSON
        ).v2();
        @SuppressWarnings("unchecked")
        Map<String, Object> multiFields = (Map<String, Object>) XContentMapValues.extractValue("_doc.properties.field.fields", sourceAsMap);
        assertThat(multiFields.size(), equalTo(multiFieldNames.length));

        int i = 0;
        // underlying map is LinkedHashMap, so this ok:
        for (String field : multiFields.keySet()) {
            assertThat(field, equalTo(multiFieldNames[i++]));
        }
    }

    public void testObjectFieldNotAllowed() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("multi").field("type", "object").endObject();
            b.endObject();
        })));
        assertThat(exception.getMessage(), containsString("cannot be used in multi field"));
    }

    public void testNestedFieldNotAllowed() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("multi").field("type", "nested").endObject();
            b.endObject();
        })));
        assertThat(exception.getMessage(), containsString("cannot be used in multi field"));
    }

    public void testMultiFieldWithDot() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("raw.foo").field("type", "text").endObject();
            b.endObject();
        })));
        assertThat(
            exception.getMessage(),
            equalTo("Failed to parse mapping: Field name [raw.foo] which is a multi field of [field] cannot contain '.'")
        );
    }

    public void testUnknownLegacyFieldsUnderKnownRootField() throws Exception {
        MapperService service = createMapperService(Version.fromString("5.0.0"), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("name");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("subfield").field("type", "unknown").endObject();
            b.endObject();
            b.endObject();
        }));
        assertThat(service.mappedField("name.subfield").type(), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
    }

    public void testUnmappedLegacyFieldsUnderKnownRootField() throws Exception {
        MapperService service = createMapperService(Version.fromString("5.0.0"), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("name");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("subfield").field("type", CompletionFieldMapper.CONTENT_TYPE).endObject();
            b.endObject();
            b.endObject();
        }));
        assertThat(service.mappedField("name.subfield").type(), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
    }

    public void testFieldsUnderUnknownRootField() throws Exception {
        MapperService service = createMapperService(Version.fromString("5.0.0"), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("name");
            b.field("type", "unknown");
            b.startObject("fields");
            b.startObject("subfield").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        assertThat(service.mappedField("name").type(), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertThat(service.mappedField("name.subfield").type(), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
    }

    public void testFieldsUnderUnmappedRootField() throws Exception {
        MapperService service = createMapperService(Version.fromString("5.0.0"), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("name");
            b.field("type", CompletionFieldMapper.CONTENT_TYPE);
            b.startObject("fields");
            b.startObject("subfield").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        assertThat(service.mappedField("name").type(), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertThat(service.mappedField("name.subfield").type(), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
    }
}
