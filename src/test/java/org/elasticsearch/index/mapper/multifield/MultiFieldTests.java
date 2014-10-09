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

package org.elasticsearch.index.mapper.multifield;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class MultiFieldTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testMultiField_multiFieldType() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-multi-field-type.json");
        testMultiField(mapping);
    }

    @Test
    public void testMultiField_multiFields() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-multi-fields.json");
        testMultiField(mapping);
    }

    private void testMultiField(String mapping) throws Exception {
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        Document doc = docMapper.parse(json).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.indexed");
        assertThat(f.name(), equalTo("name.indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.not_indexed");
        assertThat(f.name(), equalTo("name.not_indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(false));

        f = doc.getField("object1.multi1");
        assertThat(f.name(), equalTo("object1.multi1"));

        f = doc.getField("object1.multi1.string");
        assertThat(f.name(), equalTo("object1.multi1.string"));
        assertThat(f.stringValue(), equalTo("2010-01-01"));

        assertThat(docMapper.mappers().fullName("name").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name").mapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name").mapper().fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().fullName("name.indexed").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name.indexed").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name.indexed").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.indexed").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("name.indexed").mapper().fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper().fieldType().indexed(), equalTo(false));
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper().fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().fullName("name.test1").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name.test1").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name.test1").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.test1").mapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.test1").mapper().fieldType().tokenized(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.test1").mapper().fieldDataType().getLoading(), equalTo(FieldMapper.Loading.EAGER));

        assertThat(docMapper.mappers().fullName("name.test2").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name.test2").mapper(), instanceOf(TokenCountFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name.test2").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.test2").mapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.test2").mapper().fieldType().tokenized(), equalTo(false));
        assertThat(((TokenCountFieldMapper) docMapper.mappers().fullName("name.test2").mapper()).analyzer(), equalTo("simple"));
        assertThat(((TokenCountFieldMapper) docMapper.mappers().fullName("name.test2").mapper()).analyzer(), equalTo("simple"));

        assertThat(docMapper.mappers().fullName("object1.multi1").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("object1.multi1").mapper(), instanceOf(DateFieldMapper.class));
        assertThat(docMapper.mappers().fullName("object1.multi1.string").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("object1.multi1.string").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("object1.multi1.string").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("object1.multi1.string").mapper().fieldType().tokenized(), equalTo(false));
    }

    @Test
    public void testBuildThenParse() throws Exception {
        IndexService indexService = createIndex("test");
        Settings settings = indexService.settingsService().getSettings();
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();

        DocumentMapper builderDocMapper = doc("test", settings, rootObject("person").add(
                stringField("name").store(true)
                        .addMultiField(stringField("indexed").index(true).tokenized(true))
                        .addMultiField(stringField("not_indexed").index(false).store(true))
        )).build(mapperParser);
        builderDocMapper.refreshSource();

        String builtMapping = builderDocMapper.mappingSource().string();
//        System.out.println(builtMapping);
        // reparse it
        DocumentMapper docMapper = mapperParser.parse(builtMapping);


        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        Document doc = docMapper.parse(json).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.indexed");
        assertThat(f.name(), equalTo("name.indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().tokenized(), equalTo(true));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.not_indexed");
        assertThat(f.name(), equalTo("name.not_indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(false));
    }

    @Test
    public void testConvertMultiFieldNoDefaultField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-multi-field-type-no-default-field.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        Document doc = docMapper.parse(json).rootDoc();

        assertNull(doc.getField("name"));
        IndexableField f = doc.getField("name.indexed");
        assertThat(f.name(), equalTo("name.indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.not_indexed");
        assertThat(f.name(), equalTo("name.not_indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(false));

        assertThat(docMapper.mappers().fullName("name").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name").mapper().fieldType().indexed(), equalTo(false));
        assertThat(docMapper.mappers().fullName("name").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("name").mapper().fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().fullName("name.indexed").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name.indexed").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name.indexed").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.indexed").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("name.indexed").mapper().fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper().fieldType().indexed(), equalTo(false));
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.not_indexed").mapper().fieldType().tokenized(), equalTo(true));

        assertNull(doc.getField("age"));
        f = doc.getField("age.not_stored");
        assertThat(f.name(), equalTo("age.not_stored"));
        assertThat(f.numericValue(), equalTo((Number) 28L));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("age.stored");
        assertThat(f.name(), equalTo("age.stored"));
        assertThat(f.numericValue(), equalTo((Number) 28L));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(true));

        assertThat(docMapper.mappers().fullName("age").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("age").mapper(), instanceOf(LongFieldMapper.class));
        assertThat(docMapper.mappers().fullName("age").mapper().fieldType().indexed(), equalTo(false));
        assertThat(docMapper.mappers().fullName("age").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("age").mapper().fieldType().tokenized(), equalTo(false));

        assertThat(docMapper.mappers().fullName("age.not_stored").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("age.not_stored").mapper(), instanceOf(LongFieldMapper.class));
        assertThat(docMapper.mappers().fullName("age.not_stored").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("age.not_stored").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("age.not_stored").mapper().fieldType().tokenized(), equalTo(false));

        assertThat(docMapper.mappers().fullName("age.stored").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("age.stored").mapper(), instanceOf(LongFieldMapper.class));
        assertThat(docMapper.mappers().fullName("age.stored").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("age.stored").mapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.mappers().fullName("age.stored").mapper().fieldType().tokenized(), equalTo(false));
    }

    @Test
    public void testConvertMultiFieldGeoPoint() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-multi-field-type-geo_point.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.mappers().fullName("a").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("a").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("a").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("a").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("a").mapper().fieldType().tokenized(), equalTo(false));

        assertThat(docMapper.mappers().fullName("a.b").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("a.b").mapper(), instanceOf(GeoPointFieldMapper.class));
        assertThat(docMapper.mappers().fullName("a.b").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("a.b").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("a.b").mapper().fieldType().tokenized(), equalTo(false));

        BytesReference json = jsonBuilder().startObject()
                .field("_id", "1")
                .field("a", "-1,-1")
                .endObject().bytes();
        Document doc = docMapper.parse(json).rootDoc();

        IndexableField f = doc.getField("a");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("a"));
        assertThat(f.stringValue(), equalTo("-1,-1"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("a.b");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("a.b"));
        assertThat(f.stringValue(), equalTo("-1.0,-1.0"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        assertThat(docMapper.mappers().fullName("b").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("b").mapper(), instanceOf(GeoPointFieldMapper.class));
        assertThat(docMapper.mappers().fullName("b").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("b").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("b").mapper().fieldType().tokenized(), equalTo(false));

        assertThat(docMapper.mappers().fullName("b.a").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("b.a").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("b.a").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("b.a").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("b.a").mapper().fieldType().tokenized(), equalTo(false));

        json = jsonBuilder().startObject()
                .field("_id", "1")
                .field("b", "-1,-1")
                .endObject().bytes();
        doc = docMapper.parse(json).rootDoc();

        f = doc.getField("b");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("b"));
        assertThat(f.stringValue(), equalTo("-1.0,-1.0"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("b.a");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("b.a"));
        assertThat(f.stringValue(), equalTo("-1,-1"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        json = jsonBuilder().startObject()
                .field("_id", "1")
                .startArray("b").startArray().value(-1).value(-1).endArray().startArray().value(-2).value(-2).endArray().endArray()
                .endObject().bytes();
        doc = docMapper.parse(json).rootDoc();

        f = doc.getFields("b")[0];
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("b"));
        assertThat(f.stringValue(), equalTo("-1.0,-1.0"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getFields("b")[1];
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("b"));
        assertThat(f.stringValue(), equalTo("-2.0,-2.0"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("b.a");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("b.a"));
        // NOTE: "]" B/c the lat,long aren't specified as a string, we miss the actual values when parsing the multi
        // fields. We already skipped over the coordinates values and can't get to the coordinates.
        // This happens if coordinates are specified as array and object.
        assertThat(f.stringValue(), equalTo("]"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));
    }

    @Test
    public void testConvertMultiFieldCompletion() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-multi-field-type-completion.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat(docMapper.mappers().fullName("a").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("a").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("a").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("a").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("a").mapper().fieldType().tokenized(), equalTo(false));

        assertThat(docMapper.mappers().fullName("a.b").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("a.b").mapper(), instanceOf(CompletionFieldMapper.class));
        assertThat(docMapper.mappers().fullName("a.b").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("a.b").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("a.b").mapper().fieldType().tokenized(), equalTo(true));

        BytesReference json = jsonBuilder().startObject()
                .field("_id", "1")
                .field("a", "complete me")
                .endObject().bytes();
        Document doc = docMapper.parse(json).rootDoc();

        IndexableField f = doc.getField("a");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("a"));
        assertThat(f.stringValue(), equalTo("complete me"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("a.b");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("a.b"));
        assertThat(f.stringValue(), equalTo("complete me"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        assertThat(docMapper.mappers().fullName("b").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("b").mapper(), instanceOf(CompletionFieldMapper.class));
        assertThat(docMapper.mappers().fullName("b").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("b").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("b").mapper().fieldType().tokenized(), equalTo(true));

        assertThat(docMapper.mappers().fullName("b.a").mapper(), notNullValue());
        assertThat(docMapper.mappers().fullName("b.a").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("b.a").mapper().fieldType().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("b.a").mapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.mappers().fullName("b.a").mapper().fieldType().tokenized(), equalTo(false));

        json = jsonBuilder().startObject()
                .field("_id", "1")
                .field("b", "complete me")
                .endObject().bytes();
        doc = docMapper.parse(json).rootDoc();

        f = doc.getField("b");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("b"));
        assertThat(f.stringValue(), equalTo("complete me"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("b.a");
        assertThat(f, notNullValue());
        assertThat(f.name(), equalTo("b.a"));
        assertThat(f.stringValue(), equalTo("complete me"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));
    }

    @Test
    // The underlying order of the fields in multi fields in the mapping source should always be consistent, if not this
    // can to unnecessary re-syncing of the mappings between the local instance and cluster state
    public void testMultiFieldsInConsistentOrder() throws Exception {
        String[] multiFieldNames = new String[randomIntBetween(2, 10)];
        for (int i = 0; i < multiFieldNames.length; i++) {
            multiFieldNames[i] = randomAsciiOfLength(4);
        }

        XContentBuilder builder = jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("my_field").field("type", "string").startObject("fields");
        for (String multiFieldName : multiFieldNames) {
            builder = builder.startObject(multiFieldName).field("type", "string").endObject();
        }
        builder = builder.endObject().endObject().endObject().endObject().endObject();
        String mapping = builder.string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        Arrays.sort(multiFieldNames);

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(docMapper.mappingSource().compressed(), true).v2();
        @SuppressWarnings("unchecked")
        Map<String, Object> multiFields = (Map<String, Object>) XContentMapValues.extractValue("type.properties.my_field.fields", sourceAsMap);
        assertThat(multiFields.size(), equalTo(multiFieldNames.length));

        int i = 0;
        // underlying map is LinkedHashMap, so this ok:
        for (String field : multiFields.keySet()) {
            assertThat(field, equalTo(multiFieldNames[i++]));
        }
    }
}
