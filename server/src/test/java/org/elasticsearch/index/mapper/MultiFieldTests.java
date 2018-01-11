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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class MultiFieldTests extends ESSingleNodeTestCase {

    public void testMultiFieldMultiFields() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-multi-fields.json");
        testMultiField(mapping);
    }

    private void testMultiField(String mapping) throws Exception {
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        Document doc = docMapper.parse(SourceToParse.source("test", "person", "1", json, XContentType.JSON)).rootDoc();

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

        assertThat(docMapper.mappers().getMapper("name"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name"), instanceOf(TextFieldMapper.class));
        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name").fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().getMapper("name").fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.indexed"), instanceOf(TextFieldMapper.class));
        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name.indexed").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed").fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().getMapper("name.indexed").fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), instanceOf(TextFieldMapper.class));
        assertEquals(IndexOptions.NONE, docMapper.mappers().getMapper("name.not_indexed").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.not_indexed").fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().getMapper("name.not_indexed").fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().getMapper("name.test1"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.test1"), instanceOf(TextFieldMapper.class));
        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name.test1").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.test1").fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().getMapper("name.test1").fieldType().tokenized(), equalTo(true));
        assertThat(docMapper.mappers().getMapper("name.test1").fieldType().eagerGlobalOrdinals(), equalTo(true));

        assertThat(docMapper.mappers().getMapper("object1.multi1"), notNullValue());
        assertThat(docMapper.mappers().getMapper("object1.multi1"), instanceOf(DateFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("object1.multi1.string"), notNullValue());
        assertThat(docMapper.mappers().getMapper("object1.multi1.string"), instanceOf(KeywordFieldMapper.class));
        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("object1.multi1.string").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("object1.multi1.string").fieldType().tokenized(), equalTo(false));
    }

    public void testBuildThenParse() throws Exception {
        IndexService indexService = createIndex("test");

        DocumentMapper builderDocMapper = new DocumentMapper.Builder(new RootObjectMapper.Builder("person").add(
                new TextFieldMapper.Builder("name").store(true)
                        .addMultiField(new TextFieldMapper.Builder("indexed").index(true).tokenized(true))
                        .addMultiField(new TextFieldMapper.Builder("not_indexed").index(false).store(true))
        ), indexService.mapperService()).build(indexService.mapperService());

        String builtMapping = builderDocMapper.mappingSource().string();
        // reparse it
        DocumentMapper docMapper = indexService.mapperService().documentMapperParser().parse("person", new CompressedXContent(builtMapping));


        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        Document doc = docMapper.parse(SourceToParse.source("test", "person", "1", json, XContentType.JSON)).rootDoc();

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
        for (int i = 0; i < multiFieldNames.length; i++) {
            multiFieldNames[i] = randomAlphaOfLength(4);
        }

        XContentBuilder builder = jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("my_field").field("type", "text").startObject("fields");
        for (String multiFieldName : multiFieldNames) {
            builder = builder.startObject(multiFieldName).field("type", "text").endObject();
        }
        builder = builder.endObject().endObject().endObject().endObject().endObject();
        String mapping = builder.string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        Arrays.sort(multiFieldNames);

        Map<String, Object> sourceAsMap =
            XContentHelper.convertToMap(docMapper.mappingSource().compressedReference(), true, builder.contentType()).v2();
        @SuppressWarnings("unchecked")
        Map<String, Object> multiFields = (Map<String, Object>) XContentMapValues.extractValue("type.properties.my_field.fields", sourceAsMap);
        assertThat(multiFields.size(), equalTo(multiFieldNames.length));

        int i = 0;
        // underlying map is LinkedHashMap, so this ok:
        for (String field : multiFields.keySet()) {
            assertThat(field, equalTo(multiFieldNames[i++]));
        }
    }

    public void testObjectFieldNotAllowed() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type").startObject("properties").startObject("my_field")
            .field("type", "text").startObject("fields").startObject("multi").field("type", "object").endObject().endObject()
            .endObject().endObject().endObject().endObject().string();
        final DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected mapping parse failure");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("cannot be used in multi field"));
        }
    }

    public void testNestedFieldNotAllowed() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type").startObject("properties").startObject("my_field")
            .field("type", "text").startObject("fields").startObject("multi").field("type", "nested").endObject().endObject()
            .endObject().endObject().endObject().endObject().string();
        final DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected mapping parse failure");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("cannot be used in multi field"));
        }
    }

    public void testMultiFieldWithDot() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject()
                .startObject("my_type")
                .startObject("properties")
                .startObject("city")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw.foo")
                .field("type", "text")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        MapperService mapperService = createIndex("test").mapperService();
        try {
            mapperService.documentMapperParser().parse("my_type", new CompressedXContent(mapping.string()));
            fail("this should throw an exception because one field contains a dot");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("Field name [raw.foo] which is a multi field of [city] cannot contain '.'"));
        }
    }
}
