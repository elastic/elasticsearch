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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DynamicMappingTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testDynamicTrue() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type")
                .field("dynamic", "true")
                .startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
                        .startObject()
                        .field("field1", "value1")
                        .field("field2", "value2")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));
    }

    public void testDynamicFalse() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(jsonBuilder()
                        .startObject()
                        .field("field1", "value1")
                        .field("field2", "value2")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), nullValue());
    }


    public void testDynamicStrict() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(jsonBuilder()
                        .startObject()
                        .field("field1", "value1")
                        .field("field2", "value2")
                        .endObject()),
                XContentType.JSON)));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [type] is not allowed"));

        e = expectThrows(StrictDynamicMappingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("field1", "value1")
                                .field("field2", (String) null)
                                .endObject()),
                    XContentType.JSON)));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [type] is not allowed"));
    }

    public void testDynamicFalseWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(jsonBuilder()
                        .startObject().startObject("obj1")
                        .field("field1", "value1")
                        .field("field2", "value2")
                        .endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("obj1.field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("obj1.field2"), nullValue());
    }

    public void testDynamicStrictWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () ->
            defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(jsonBuilder()
                            .startObject().startObject("obj1")
                            .field("field1", "value1")
                            .field("field2", "value2")
                            .endObject()
                            .endObject()),
                    XContentType.JSON)));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [obj1] is not allowed"));
    }

    public void testDynamicMappingOnEmptyString() throws Exception {
        IndexService service = createIndex("test");
        client().prepareIndex("test").setSource("empty_field", "").get();
        MappedFieldType fieldType = service.mapperService().fullName("empty_field");
        assertNotNull(fieldType);
    }

    private String serialize(ToXContent mapper) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, new ToXContent.MapParams(emptyMap()));
        return Strings.toString(builder.endObject());
    }

    private Mapper parse(DocumentMapper mapper, DocumentMapperParser parser, XContentBuilder builder) throws Exception {
        IndexMetaData build = IndexMetaData.builder("")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0).build();
        IndexSettings settings = new IndexSettings(build, Settings.EMPTY);
        SourceToParse source = new SourceToParse("test", "some_id",
            BytesReference.bytes(builder), builder.contentType());
        try (XContentParser xContentParser = createParser(JsonXContent.jsonXContent, source.source())) {
            ParseContext.InternalParseContext ctx = new ParseContext.InternalParseContext(settings, parser, mapper, source, xContentParser);
            assertEquals(XContentParser.Token.START_OBJECT, ctx.parser().nextToken());
            ctx.parser().nextToken();
            DocumentParser.parseObjectOrNested(ctx, mapper.root());
            Mapping mapping = DocumentParser.createDynamicUpdate(mapper.mapping(), mapper, ctx.getDynamicMappers());
            return mapping == null ? null : mapping.root();
        }
    }

    public void testDynamicMappingsNotNeeded() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "text").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        // foo is already defined in the mappings
        assertNull(update);
    }

    public void testField() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo")
                    .field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                        .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()), serialize(update));
    }

    public void testIncremental() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        // Make sure that mapping updates are incremental, this is important for performance otherwise
        // every new field introduction runs in linear time with the total number of fields
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "text").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar")
            .field("bar", "baz").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                // foo is NOT in the update
                .startObject("bar").field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                            .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()), serialize(update));
    }

    public void testIntroduceTwoFields() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar")
            .field("bar", "baz").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
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
                .endObject().endObject().endObject()), serialize(update));
    }

    public void testObject() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo").startObject("bar")
            .field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties").startObject("bar").startObject("properties").startObject("baz")
                .field("type", "text")
                .startObject("fields").startObject("keyword").field("type", "keyword")
                .field("ignore_above", 256).endObject()
                .endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject()), serialize(update));
    }

    public void testArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject()
            .startArray("foo").value("bar").value("baz").endArray().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo")
                    .field("type", "text")
                    .startObject("fields")
                        .startObject("keyword")
                        .field("type", "keyword")
                            .field("ignore_above", 256)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()), serialize(update));
    }

    public void testInnerDynamicMapping() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type") .startObject("properties")
                .startObject("foo").field("type", "object").endObject()
                .endObject().endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo")
            .startObject("bar").field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties").startObject("bar").startObject("properties")
                .startObject("baz").field("type", "text").startObject("fields")
                .startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject()
                .endObject().endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject()), serialize(update));
    }

    public void testComplexArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startArray("foo")
                .startObject().field("bar", "baz").endObject()
                .startObject().field("baz", 3).endObject()
                .endArray().endObject());
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
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
                .endObject().endObject().endObject()), serialize(update));
    }

    public void testReuseExistingMappings() throws Exception {

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
        DocumentMapper newMapper = indexService.mapperService().documentMapperWithAutoCreate().getDocumentMapper();
        Mapper update = parse(newMapper, indexService.mapperService().documentMapperParser(),
            XContentFactory.jsonBuilder().startObject()
                .field("my_field1", 42)
                .field("my_field2", 43)
                .field("my_field3", 44)
                .field("my_field4", 45)
                .field("my_field5", 46)
                .field("my_field6", Instant.now().toEpochMilli())
                .field("my_field7", true)
                .endObject());
        assertNull(update);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            parse(newMapper, indexService.mapperService().documentMapperParser(),
                XContentFactory.jsonBuilder().startObject().field("my_field2", "foobar").endObject());
        });
        assertThat(e.getMessage(), containsString("failed to parse field [my_field2] of type [integer]"));
    }

    public void testMixTemplateMultiFieldAndMappingReuse() throws Exception {
        IndexService indexService = createIndex("test");
        XContentBuilder mappings1 = jsonBuilder().startObject()
                .startObject("_doc")
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
        indexService.mapperService().merge("_doc", new CompressedXContent(BytesReference.bytes(mappings1)),
            MapperService.MergeReason.MAPPING_UPDATE);

        XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                    .field("field", "foo")
                .endObject();
        SourceToParse source = new SourceToParse("test",  "1", BytesReference.bytes(json), json.contentType());
        DocumentMapper mapper = indexService.mapperService().documentMapper();
        assertNull(mapper.mappers().getMapper("field.raw"));
        ParsedDocument parsed = mapper.parse(source);
        assertNotNull(parsed.dynamicMappingsUpdate());

        indexService.mapperService().merge("_doc", new CompressedXContent(parsed.dynamicMappingsUpdate().toString()),
            MapperService.MergeReason.MAPPING_UPDATE);
        mapper = indexService.mapperService().documentMapper();
        assertNotNull(mapper.mappers().getMapper("field.raw"));
        parsed = mapper.parse(source);
        assertNull(parsed.dynamicMappingsUpdate());
    }

    public void testDefaultFloatingPointMappings() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        String mapping = Strings.toString(jsonBuilder().startObject()
                .startObject("type")
                    .field("numeric_detection", true)
                .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        DocumentMapper mapper = mapperService.documentMapper();
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.jsonBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.yamlBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.smileBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.cborBuilder());
    }

    private void doTestDefaultFloatingPointMappings(DocumentMapper mapper, XContentBuilder builder) throws IOException {
        BytesReference source = BytesReference.bytes(builder.startObject()
                .field("foo", 3.2f) // float
                .field("bar", 3.2d) // double
                .field("baz", (double) 3.2f) // double that can be accurately represented as a float
                .field("quux", "3.2") // float detected through numeric detection
                .endObject());
        ParsedDocument parsedDocument = mapper.parse(new SourceToParse("index", "id",
            source, builder.contentType()));
        Mapping update = parsedDocument.dynamicMappingsUpdate();
        assertNotNull(update);
        assertThat(((FieldMapper) update.root().getMapper("foo")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("bar")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("baz")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("quux")).fieldType().typeName(), equalTo("float"));
    }

    public void testNumericDetectionEnabled() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .field("numeric_detection", true)
                .endObject().endObject());

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper();

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("s_long", "100")
                        .field("s_double", "100.0")
                        .endObject()),
                XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test")
            .setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();

        defaultMapper = index.mapperService().documentMapper();
        Mapper mapper = defaultMapper.mappers().getMapper("s_long");
        assertThat(mapper.typeName(), equalTo("long"));

        mapper = defaultMapper.mappers().getMapper("s_double");
        assertThat(mapper.typeName(), equalTo("float"));
    }

    public void testNumericDetectionDefault() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .endObject().endObject());

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper();

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("s_long", "100")
                        .field("s_double", "100.0")
                        .endObject()),
                XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertAcked(client().admin().indices().preparePutMapping("test")
            .setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get());

        defaultMapper = index.mapperService().documentMapper();
        Mapper mapper = defaultMapper.mappers().getMapper("s_long");
        assertThat(mapper, instanceOf(TextFieldMapper.class));

        mapper = defaultMapper.mappers().getMapper("s_double");
        assertThat(mapper, instanceOf(TextFieldMapper.class));
    }

    public void testDateDetectionInheritsFormat() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startArray("dynamic_date_formats")
                    .value("yyyy-MM-dd")
                .endArray()
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("dates")
                            .field("match_mapping_type", "date")
                            .field("match", "*2")
                            .startObject("mapping")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject()
                        .startObject("dates")
                            .field("match_mapping_type", "date")
                            .field("match", "*3")
                            .startObject("mapping")
                                .field("format", "yyyy-MM-dd||epoch_millis")
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray()
                .endObject().endObject());

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper();

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("date1", "2016-11-20")
                            .field("date2", "2016-11-20")
                            .field("date3", "2016-11-20")
                        .endObject()),
                XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertAcked(client().admin().indices().preparePutMapping("test")
            .setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get());

        defaultMapper = index.mapperService().documentMapper();

        DateFieldMapper dateMapper1 = (DateFieldMapper) defaultMapper.mappers().getMapper("date1");
        DateFieldMapper dateMapper2 = (DateFieldMapper) defaultMapper.mappers().getMapper("date2");
        DateFieldMapper dateMapper3 = (DateFieldMapper) defaultMapper.mappers().getMapper("date3");
        // inherited from dynamic date format
        assertEquals("yyyy-MM-dd", dateMapper1.fieldType().dateTimeFormatter().pattern());
        // inherited from dynamic date format since the mapping in the template did not specify a format
        assertEquals("yyyy-MM-dd", dateMapper2.fieldType().dateTimeFormatter().pattern());
        // not inherited from the dynamic date format since the template defined an explicit format
        assertEquals("yyyy-MM-dd||epoch_millis", dateMapper3.fieldType().dateTimeFormatter().pattern());
    }

    public void testDynamicTemplateOrder() throws IOException {
        // https://github.com/elastic/elasticsearch/issues/18625
        // elasticsearch used to apply templates that do not have a match_mapping_type first
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
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
        IndexService index = createIndex("test", Settings.EMPTY, mapping);
        client().prepareIndex("test").setId("1").setSource("foo", "abc").get();
        assertThat(index.mapperService().fullName("foo"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
    }

    public void testMappingVersionAfterDynamicMappingUpdate() {
        createIndex("test", client().admin().indices().prepareCreate("test"));
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final long previousVersion = clusterService.state().metaData().index("test").getMappingVersion();
        client().prepareIndex("test").setId("1").setSource("field", "text").get();
        assertThat(clusterService.state().metaData().index("test").getMappingVersion(), equalTo(1 + previousVersion));
    }

}
