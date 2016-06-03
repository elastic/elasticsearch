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
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper.BooleanFieldType;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.KeywordFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DynamicMappingTests extends ESSingleNodeTestCase {

    public void testDynamicTrue() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "true")
                .startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));
    }

    public void testDynamicFalse() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), nullValue());
    }


    public void testDynamicStrict() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse("test", "type", "1", jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject()
                .bytes()));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [type] is not allowed"));

        e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field1", "value1")
                    .field("field2", (String) null)
                    .endObject()
                    .bytes()));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [type] is not allowed"));
    }

    public void testDynamicFalseWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", jsonBuilder()
                .startObject().startObject("obj1")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("obj1.field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("obj1.field2"), nullValue());
    }

    public void testDynamicStrictWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () ->
            defaultMapper.parse("test", "type", "1", jsonBuilder()
                    .startObject().startObject("obj1")
                    .field("field1", "value1")
                    .field("field2", "value2")
                    .endObject()
                    .endObject()
                    .bytes()));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [obj1] is not allowed"));
    }

    public void testDynamicMappingOnEmptyString() throws Exception {
        IndexService service = createIndex("test");
        client().prepareIndex("test", "type").setSource("empty_field", "").get();
        MappedFieldType fieldType = service.mapperService().fullName("empty_field");
        assertNotNull(fieldType);
    }

    public void testTypeNotCreatedOnIndexFailure() throws IOException, InterruptedException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_default_")
                .field("dynamic", "strict")
                .endObject().endObject();

        IndexService indexService = createIndex("test", Settings.EMPTY, "_default_", mapping);

        try {
            client().prepareIndex().setIndex("test").setType("type").setSource(jsonBuilder().startObject().field("test", "test").endObject()).get();
            fail();
        } catch (StrictDynamicMappingException e) {

        }

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertNull(getMappingsResponse.getMappings().get("test").get("type"));
    }

    private String serialize(ToXContent mapper) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, new ToXContent.MapParams(emptyMap()));
        return builder.endObject().string();
    }

    private Mapper parse(DocumentMapper mapper, DocumentMapperParser parser, XContentBuilder builder) throws Exception {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        SourceToParse source = SourceToParse.source("test", mapper.type(), "some_id", builder.bytes());
        ParseContext.InternalParseContext ctx = new ParseContext.InternalParseContext(settings, parser, mapper, source, XContentHelper.createParser(source.source()));
        assertEquals(XContentParser.Token.START_OBJECT, ctx.parser().nextToken());
        ctx.parser().nextToken();
        DocumentParser.parseObjectOrNested(ctx, mapper.root(), true);
        Mapping mapping = DocumentParser.createDynamicUpdate(mapper.mapping(), mapper, ctx.getDynamicMappers());
        return mapping == null ? null : mapping.root();
    }

    public void testDynamicMappingsNotNeeded() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        // foo is already defined in the mappings
        assertNull(update);
    }

    public void testField() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo")
                    .field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                        .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testIncremental() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        // Make sure that mapping updates are incremental, this is important for performance otherwise
        // every new field introduction runs in linear time with the total number of fields
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").field("bar", "baz").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                // foo is NOT in the update
                .startObject("bar").field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                            .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testIntroduceTwoFields() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").field("bar", "baz").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("bar").field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                            .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("foo").field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                            .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testObject() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo").startObject("bar").field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties").startObject("bar").startObject("properties").startObject("baz").field("type", "text")
                .startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject()
                .endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startArray("foo").value("bar").value("baz").endArray().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo")
                    .field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                        .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testInnerDynamicMapping() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type") .startObject("properties")
                .startObject("foo").field("type", "object").endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo").startObject("bar").field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties").startObject("bar").startObject("properties").startObject("baz").field("type", "text").startObject("fields")
                .startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject()
                .endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testComplexArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startArray("foo")
                .startObject().field("bar", "baz").endObject()
                .startObject().field("baz", 3).endObject()
                .endArray().endObject());
        assertEquals(mapping, serialize(mapper));
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties")
                .startObject("bar").field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                            .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("baz").field("type", "long").endObject()
                .endObject().endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testReuseExistingMappings() throws IOException, Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY, "type",
                "my_field1", "type=text,store=true",
                "my_field2", "type=integer,store=false",
                "my_field3", "type=long,doc_values=false",
                "my_field4", "type=float,index=false",
                "my_field5", "type=double,store=true",
                "my_field6", "type=date,doc_values=false",
                "my_field7", "type=boolean,doc_values=false");

        // Even if the dynamic type of our new field is long, we already have a mapping for the same field
        // of type string so it should be mapped as a string
        DocumentMapper newMapper = indexService.mapperService().documentMapperWithAutoCreate("type2").getDocumentMapper();
        Mapper update = parse(newMapper, indexService.mapperService().documentMapperParser(),
                XContentFactory.jsonBuilder().startObject()
                    .field("my_field1", 42)
                    .field("my_field2", 43)
                    .field("my_field3", 44)
                    .field("my_field4", 45)
                    .field("my_field5", 46)
                    .field("my_field6", 47)
                    .field("my_field7", true)
                .endObject());
        Mapper myField1Mapper = null;
        Mapper myField2Mapper = null;
        Mapper myField3Mapper = null;
        Mapper myField4Mapper = null;
        Mapper myField5Mapper = null;
        Mapper myField6Mapper = null;
        Mapper myField7Mapper = null;
        for (Mapper m : update) {
            switch (m.name()) {
            case "my_field1":
                myField1Mapper = m;
                break;
            case "my_field2":
                myField2Mapper = m;
                break;
            case "my_field3":
                myField3Mapper = m;
                break;
            case "my_field4":
                myField4Mapper = m;
                break;
            case "my_field5":
                myField5Mapper = m;
                break;
            case "my_field6":
                myField6Mapper = m;
                break;
            case "my_field7":
                myField7Mapper = m;
                break;
            }
        }
        assertNotNull(myField1Mapper);
        // same type
        assertTrue(myField1Mapper instanceof TextFieldMapper);
        // and same option
        assertTrue(((TextFieldMapper) myField1Mapper).fieldType().stored());

        // Even if dynamic mappings would map a numeric field as a long, here it should map it as a integer
        // since we already have a mapping of type integer
        assertNotNull(myField2Mapper);
        // same type
        assertEquals("integer", ((FieldMapper) myField2Mapper).fieldType().typeName());
        // and same option
        assertFalse(((FieldMapper) myField2Mapper).fieldType().stored());

        assertNotNull(myField3Mapper);
        assertTrue(myField3Mapper instanceof NumberFieldMapper);
        assertFalse(((NumberFieldType) ((NumberFieldMapper) myField3Mapper).fieldType()).hasDocValues());

        assertNotNull(myField4Mapper);
        assertTrue(myField4Mapper instanceof NumberFieldMapper);
        assertEquals(IndexOptions.NONE, ((FieldMapper) myField4Mapper).fieldType().indexOptions());

        assertNotNull(myField5Mapper);

        assertTrue(myField5Mapper instanceof NumberFieldMapper);
        assertTrue(((NumberFieldMapper) myField5Mapper).fieldType().stored());

        assertNotNull(myField6Mapper);
        assertTrue(myField6Mapper instanceof DateFieldMapper);
        assertFalse(((DateFieldType) ((DateFieldMapper) myField6Mapper).fieldType()).hasDocValues());

        assertNotNull(myField7Mapper);
        assertTrue(myField7Mapper instanceof BooleanFieldMapper);
        assertFalse(((BooleanFieldType) ((BooleanFieldMapper) myField7Mapper).fieldType()).hasDocValues());

        // This can't work
        try {
            parse(newMapper, indexService.mapperService().documentMapperParser(),
                    XContentFactory.jsonBuilder().startObject().field("my_field2", "foobar").endObject());
            fail("Cannot succeed, incompatible types");
        } catch (MapperParsingException e) {
            // expected
        }
    }

    public void testMixTemplateMultiFieldAndMappingReuse() throws Exception {
        IndexService indexService = createIndex("test");
        XContentBuilder mappings1 = jsonBuilder().startObject()
                .startObject("type1")
                    .startArray("dynamic_templates")
                        .startObject()
                            .startObject("template1")
                                .field("match_mapping_type", "string")
                                .startObject("mapping")
                                    .field("type", "text")
                                    .startObject("fields")
                                        .startObject("raw")
                                            .field("type", "keyword")
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endArray()
                .endObject().endObject();
        indexService.mapperService().merge("type1", new CompressedXContent(mappings1.bytes()), MapperService.MergeReason.MAPPING_UPDATE, false);
        XContentBuilder mappings2 = jsonBuilder().startObject()
                .startObject("type2")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject();
        indexService.mapperService().merge("type2", new CompressedXContent(mappings2.bytes()), MapperService.MergeReason.MAPPING_UPDATE, false);

        XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                    .field("field", "foo")
                .endObject();
        SourceToParse source = SourceToParse.source("test", "type1", "1", json.bytes());
        DocumentMapper mapper = indexService.mapperService().documentMapper("type1");
        assertNull(mapper.mappers().getMapper("field.raw"));
        ParsedDocument parsed = mapper.parse(source);
        assertNotNull(parsed.dynamicMappingsUpdate());

        indexService.mapperService().merge("type1", new CompressedXContent(parsed.dynamicMappingsUpdate().toString()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapper = indexService.mapperService().documentMapper("type1");
        assertNotNull(mapper.mappers().getMapper("field.raw"));
        parsed = mapper.parse(source);
        assertNull(parsed.dynamicMappingsUpdate());
    }

    public void testDefaultFloatingPointMappings() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        String mapping = jsonBuilder().startObject()
                .startObject("type")
                    .field("numeric_detection", true)
                .endObject().endObject().string();
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        DocumentMapper mapper = mapperService.documentMapper("type");
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.jsonBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.yamlBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.smileBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.cborBuilder());
    }

    private void doTestDefaultFloatingPointMappings(DocumentMapper mapper, XContentBuilder builder) throws IOException {
        BytesReference source = builder.startObject()
                .field("foo", 3.2f) // float
                .field("bar", 3.2d) // double
                .field("baz", (double) 3.2f) // double that can be accurately represented as a float
                .field("quux", "3.2") // float detected through numeric detection
                .endObject().bytes();
        ParsedDocument parsedDocument = mapper.parse("index", "type", "id", source);
        Mapping update = parsedDocument.dynamicMappingsUpdate();
        assertNotNull(update);
        assertThat(((FieldMapper) update.root().getMapper("foo")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("bar")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("baz")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("quux")).fieldType().typeName(), equalTo("float"));
    }

    public void testNumericDetectionEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("numeric_detection", true)
                .endObject().endObject().string();

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper("type");

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_long", "100")
                .field("s_double", "100.0")
                .endObject()
                .bytes());
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test").setType("type").setSource(doc.dynamicMappingsUpdate().toString()).get();

        defaultMapper = index.mapperService().documentMapper("type");
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper.fieldType().typeName(), equalTo("long"));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper.fieldType().typeName(), equalTo("float"));
    }

    public void testNumericDetectionDefault() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper("type");

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_long", "100")
                .field("s_double", "100.0")
                .endObject()
                .bytes());
        assertNotNull(doc.dynamicMappingsUpdate());
        assertAcked(client().admin().indices().preparePutMapping("test").setType("type").setSource(doc.dynamicMappingsUpdate().toString()).get());

        defaultMapper = index.mapperService().documentMapper("type");
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper, instanceOf(TextFieldMapper.class));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper, instanceOf(TextFieldMapper.class));
    }

    public void testDynamicTemplateOrder() throws IOException {
        // https://github.com/elastic/elasticsearch/issues/18625
        // elasticsearch used to apply templates that do not have a match_mapping_type first
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("type-based")
                            .field("match_mapping_type", "string")
                            .startObject("mapping")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject()
                    .startObject("path-based")
                        .field("path_match", "foo")
                        .startObject("mapping")
                            .field("type", "long")
                        .endObject()
                    .endObject()
                .endObject()
                .endArray()
                .endObject().endObject();
        IndexService index = createIndex("test", Settings.EMPTY, "type", mapping);
        client().prepareIndex("test", "type", "1").setSource("foo", "abc").get();
        assertThat(index.mapperService().fullName("foo"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
    }
}
