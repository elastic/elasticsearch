/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DynamicTemplatesTests extends MapperServiceTestCase {

    public void testMatchTypeOnly() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("index", false).endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("s", "hello");
            b.field("l", 1);
        }));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        assertThat(mapperService.fieldType("s"), notNullValue());
        assertFalse(mapperService.fieldType("s").isIndexed());
        assertFalse(mapperService.fieldType("s").isSearchable());

        assertThat(mapperService.fieldType("l"), notNullValue());
        assertFalse(mapperService.fieldType("s").isIndexed());
        assertTrue(mapperService.fieldType("l").isSearchable());
    }

    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-mapping.json");
        MapperService mapperService = createMapperService(mapping);
        String docJson = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-data.json");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));

        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        LuceneDocument doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("some name")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("name");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1");
        assertThat(f.name(), equalTo("multi1"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi1");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1.org");
        assertThat(f.name(), equalTo("multi1.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 1")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi1.org");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2");
        assertThat(f.name(), equalTo("multi2"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi2");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2.org");
        assertThat(f.name(), equalTo("multi2.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 2")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi2.org");
        assertNotNull(fieldMapper);
    }

    public void testSimpleWithXContentTraverse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-mapping.json");
        MapperService mapperService = createMapperService(mapping);
        String docJson = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-data.json");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));

        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        LuceneDocument doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("some name")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("name");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1");
        assertThat(f.name(), equalTo("multi1"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi1");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1.org");
        assertThat(f.name(), equalTo("multi1.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 1")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi1.org");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2");
        assertThat(f.name(), equalTo("multi2"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi2");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2.org");
        assertThat(f.name(), equalTo("multi2.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 2")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("multi2.org");
        assertNotNull(fieldMapper);
    }

    public void testDynamicMapperWithBadMapping() throws IOException {
        {
            // in 7.x versions this will issue a deprecation warning
            IndexVersion version = IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersion.V_7_0_0,
                IndexVersionUtils.getPreviousVersion(IndexVersion.V_8_0_0)
            );
            DocumentMapper mapper = createDocumentMapper(version, topMapping(b -> {
                b.startArray("dynamic_templates");
                {
                    b.startObject();
                    {
                        b.startObject("test");
                        {
                            b.field("match_mapping_type", "string");
                            b.startObject("mapping").field("badparam", false).endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endArray();
            }));
            assertWarnings("""
                dynamic template [test] has invalid content [{"match_mapping_type":"string","mapping":{"badparam":false}}], \
                attempted to validate it with the following match_mapping_type: [string], last error: \
                [unknown parameter [badparam] on mapper [__dynamic__test] of type [null]]""");

            mapper.parse(source(b -> b.field("field", "foo")));
            assertWarnings(
                "Parameter [badparam] is used in a dynamic template mapping and has no effect on type [null]. "
                    + "Usage will result in an error in future major versions and should be removed."
            );
        }

        {
            // in 8.x it will error out
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping(b -> {
                b.startArray("dynamic_templates");
                {
                    b.startObject();
                    {
                        b.startObject("test");
                        {
                            b.field("match_mapping_type", "string");
                            b.startObject("mapping").field("badparam", false).endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endArray();
            })));
            assertThat(e.getMessage(), containsString("dynamic template [test] has invalid content"));
            assertThat(e.getCause().getMessage(), containsString("badparam"));
        }
    }

    public void testDynamicRuntimeWithBadMapping() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("runtime").field("badparam", false).endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        })));
        assertThat(e.getMessage(), containsString("dynamic template [test] has invalid content"));
        assertThat(e.getCause().getMessage(), containsString("badparam"));
    }

    public void testDynamicTemplates() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startArray("dynamic_templates")
                .startObject()
                .startObject("my_template")
                .field("match_mapping_type", "string")
                .startObject("mapping")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject()
                .endObject()
        );
        MapperService mapperService = createMapperService(mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = Strings.toString(
            XContentFactory.jsonBuilder().startObject().startObject(MapperService.SINGLE_MAPPING_NAME).endObject().endObject()
        );
        merge(mapperService, mapping2);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        String mapping3 = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("dynamic_templates", Collections.emptyList())
                .endObject()
                .endObject()
        );
        merge(mapperService, mapping3);
        assertEquals(mapping3, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDynamicTemplatesForIndexTemplate() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startArray("dynamic_templates")
                .startObject()
                .startObject("first_template")
                .field("path_match", "first")
                .startObject("mapping")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .startObject()
                .startObject("second_template")
                .field("path_match", "second")
                .startObject("mapping")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        MapperService mapperService = createMapperService(IndexVersion.current(), Settings.EMPTY, () -> true);
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MapperService.MergeReason.INDEX_TEMPLATE);

        // There should be no update if templates are not set.
        mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("field")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(mapping),
            MapperService.MergeReason.INDEX_TEMPLATE
        );

        DynamicTemplate[] templates = mapper.mapping().getRoot().dynamicTemplates();
        assertEquals(2, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch().get(0));
        assertEquals("second_template", templates[1].name());
        assertEquals("second", templates[1].pathMatch().get(0));

        // Dynamic templates should be appended and deduplicated.
        mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startArray("dynamic_templates")
                .startObject()
                .startObject("third_template")
                .field("path_match", "third")
                .startObject("mapping")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
                .startObject()
                .startObject("second_template")
                .field("path_match", "second_updated")
                .startObject("mapping")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(mapping),
            MapperService.MergeReason.INDEX_TEMPLATE
        );

        templates = mapper.mapping().getRoot().dynamicTemplates();
        assertEquals(3, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch().get(0));
        assertEquals("second_template", templates[1].name());
        assertEquals("second_updated", templates[1].pathMatch().get(0));
        assertEquals("third_template", templates[2].name());
        assertEquals("third", templates[2].pathMatch().get(0));
    }

    public void testIllegalDynamicTemplates() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("dynamic_templates")
                .endObject()
                .endObject()
                .endObject()
        );

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: Dynamic template syntax error. An array of named objects is expected.", e.getMessage());
    }

    public void testIllegalDynamicTemplateUnknownFieldType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getRootCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getRootCause().getMessage(), equalTo("No mapper found for type [string]"));
    }

    public void testIllegalDynamicTemplateUnknownRuntimeField() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("runtime");
                mapping.field("type", "unknown");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getRootCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getRootCause().getMessage(), equalTo("No runtime field found for type [unknown]"));
    }

    public void testIllegalDynamicTemplateUnknownAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "keyword");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getRootCause(), instanceOf(MapperParsingException.class));
        assertThat(e.getRootCause().getMessage(), equalTo("unknown parameter [foo] on mapper [__dynamic__my_template] of type [keyword]"));
    }

    public void testIllegalDynamicTemplateUnknownAttributeRuntime() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("runtime");
                mapping.field("type", "keyword");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("""
            Failed to parse mapping: dynamic template [my_template] has invalid content \
            [{"match_mapping_type":"string","runtime":{"foo":"bar","type":"keyword"}}], \
            attempted to validate it with the following match_mapping_type: [string]""", e.getMessage());
        assertEquals("unknown parameter [foo] on runtime field [__dynamic__my_template] of type [keyword]", e.getRootCause().getMessage());
    }

    public void testIllegalDynamicTemplateInvalidAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "text");
                mapping.field("analyzer", "foobar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getRootCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getRootCause().getMessage(), equalTo("analyzer [foobar] has not been configured in mappings"));
    }

    public void testIllegalDynamicTemplateNoMappingType() throws Exception {
        MapperService mapperService;
        {
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template");
                    if (randomBoolean()) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("index_phrases", true);
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();
            mapperService = createMapperService(mapping);
            assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"index_phrases\":true"));
        }
        {
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template");
                    if (randomBoolean()) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("foo", "bar");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> merge(mapperService, mapping));
            assertThat(e.getRootCause(), instanceOf(MapperParsingException.class));
            assertThat(
                e.getRootCause().getMessage(),
                equalTo("unknown parameter [foo] on mapper [__dynamic__my_template] of type [binary]")
            );
        }
    }

    public void testIllegalDynamicTemplateNoMappingTypeRuntime() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                if (randomBoolean()) {
                    mapping.field("match_mapping_type", "*");
                } else {
                    mapping.field("match", "string_*");
                }
                mapping.startObject("runtime");
                mapping.field("type", "{dynamic_type}");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getMessage(), containsString("Failed to parse mapping: dynamic template [my_template] has invalid content ["));
        assertThat(
            e.getMessage(),
            containsString("attempted to validate it with the following match_mapping_type: " + "[string, long, double, boolean, date]")
        );
        assertEquals("unknown parameter [foo] on runtime field [__dynamic__my_template] of type [date]", e.getRootCause().getMessage());
    }

    public void testIllegalDynamicTemplate7DotXIndex() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        IndexVersion createdVersion = IndexVersionUtils.randomVersionBetween(random(), IndexVersion.V_7_0_0, IndexVersion.V_7_7_0);
        MapperService mapperService = createMapperService(createdVersion, mapping);
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"type\":\"string\""));
        assertWarnings("""
            dynamic template [my_template] has invalid content \
            [{"match_mapping_type":"string","mapping":{"type":"string"}}], attempted to validate it \
            with the following match_mapping_type: [string], last error: [No mapper found for type [string]]""");
    }

    public void testTemplateWithoutMatchPredicates() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("geo_point");
                {
                    mapping.startObject("mapping");
                    mapping.field("type", "geo_point");
                    mapping.endObject();
                }
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createMapperService(mapping);
        final String json = """
            {"foo": "41.12,-71.34", "bar": "41.12,-71.34"}
            """;
        ParsedDocument doc = mapperService.documentMapper()
            .parse(new SourceToParse("1", new BytesArray(json), XContentType.JSON, null, Map.of("foo", "geo_point")));
        assertThat(doc.rootDoc().getFields("foo"), hasSize(2));
        assertThat(doc.rootDoc().getFields("bar"), hasSize(1));
    }

    public void testMixTemplateMultiFieldAndMappingReuse() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("template1");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping");
                        {
                            b.field("type", "text");
                            b.startObject("fields");
                            {
                                b.startObject("raw").field("type", "keyword").endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        assertNull(mapperService.documentMapper().mappers().getMapper("field.raw"));

        ParsedDocument parsed = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNotNull(parsed.dynamicMappingsUpdate());

        merge(mapperService, dynamicMapping(parsed.dynamicMappingsUpdate()));
        assertNotNull(mapperService.documentMapper().mappers().getMapper("field.raw"));
        parsed = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    public void testDateDetectionInheritsFormat() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_date_formats").value("yyyy-MM-dd").endArray();
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("dates");
                    {
                        b.field("match_mapping_type", "date");
                        b.field("match", "*2");
                        b.startObject("mapping").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("dates");
                    {
                        b.field("match_mapping_type", "date");
                        b.field("match", "*3");
                        b.startObject("mapping").field("format", "yyyy-MM-dd||epoch_millis").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("date1", "2016-11-20");
            b.field("date2", "2016-11-20");
            b.field("date3", "2016-11-20");
        }));
        assertNotNull(doc.dynamicMappingsUpdate());

        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        DateFieldMapper dateMapper1 = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("date1");
        DateFieldMapper dateMapper2 = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("date2");
        DateFieldMapper dateMapper3 = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("date3");
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
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("type-based");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("path-based");
                    {
                        b.field("path_match", "foo");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("foo", "abc")));
        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        assertThat(mapperService.fieldType("foo"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
    }

    public void testDynamicTemplateRuntimeMatchMappingType() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("runtime").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("s", "hello");
            b.field("l", 1);
        }));
        assertEquals(
            """
                {"_doc":{"runtime":{"s":{"type":"long"}},"properties":{"l":{"type":"long"}}}}""",
            Strings.toString(parsedDoc.dynamicMappingsUpdate())
        );
    }

    public void testDynamicTemplateRuntimeMatch() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "field*");
                        b.startObject("runtime").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("field_string", "hello");
            b.field("field_long", 1);
            b.field("field_boolean", true);
            b.field("concrete_string", "text");
            b.startObject("field_object");
            b.field("field_date", "2020-12-15");
            b.field("concrete_date", "2020-12-15");
            b.endObject();
            b.startArray("field_array");
            b.startObject();
            b.field("field_double", 1.25);
            b.field("concrete_double", 1.25);
            b.endObject();
            b.endArray();
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "runtime": {
                  "field_array.field_double": {
                    "type": "double"
                  },
                  "field_boolean": {
                    "type": "boolean"
                  },
                  "field_long": {
                    "type": "long"
                  },
                  "field_object.field_date": {
                    "type": "date"
                  },
                  "field_string": {
                    "type": "keyword"
                  }
                },
                "properties": {
                  "concrete_string": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "field_array": {
                    "properties": {
                      "concrete_double": {
                        "type": "float"
                      }
                    }
                  },
                  "field_object": {
                    "properties": {
                      "concrete_date": {
                        "type": "date"
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(parsedDoc.dynamicMappingsUpdate()));
    }

    public void testDynamicTemplateRuntimePathMatch() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("path_match", "object.*");
                        b.field("path_unmatch", "*.concrete*");
                        b.startObject("runtime").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("double", 1.23);
            b.startObject("object");
            {
                b.field("date", "2020-12-15");
                b.field("long", 1);
                b.startObject("object").field("string", "hello").field("concrete", false).endObject();
            }
            b.endObject();
            b.startObject("concrete").field("boolean", true).endObject();
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "runtime": {
                  "object.date": {
                    "type": "date"
                  },
                  "object.long": {
                    "type": "long"
                  },
                  "object.object.string": {
                    "type": "keyword"
                  }
                },
                "properties": {
                  "concrete": {
                    "properties": {
                      "boolean": {
                        "type": "boolean"
                      }
                    }
                  },
                  "double": {
                    "type": "float"
                  },
                  "object": {
                    "properties": {
                      "object": {
                        "properties": {
                          "concrete": {
                            "type": "boolean"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(parsedDoc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntimeWithDynamicTemplate() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", "runtime");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("concrete");
                    {
                        b.field("match", "concrete*");
                        b.startObject("mapping").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("double", 1.23);
            b.field("concrete_double", 1.23);
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "runtime",
                "runtime": {
                  "double": {
                    "type": "double"
                  }
                },
                "properties": {
                  "concrete_double": {
                    "type": "float"
                  }
                }
              }
            }"""), Strings.toString(parsedDoc.dynamicMappingsUpdate()));

        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", ObjectMapper.Dynamic.RUNTIME);
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument parsedDoc2 = documentMapper.parse(source(b -> {
            b.field("s", "hello");
            b.field("l", 1);
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "runtime",
                "runtime": {
                  "l": {
                    "type": "long"
                  }
                },
                "properties": {
                  "s": {
                    "type": "keyword"
                  }
                }
              }
            }"""), Strings.toString(parsedDoc2.dynamicMappingsUpdate()));
    }

    private MapperService createDynamicTemplateNoSubobjects() throws IOException {
        return createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "object");
                        b.field("match", "metric");
                        b.startObject("mapping").field("type", "object").field("subobjects", false).endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
    }

    private static void assertNoSubobjects(MapperService mapperService) {
        assertThat(mapperService.fieldType("foo.bar.baz").typeName(), equalTo("long"));
        assertNotNull(mapperService.mappingLookup().objectMappers().get("foo.bar"));
        assertThat(mapperService.fieldType("foo.metric.count").typeName(), equalTo("long"));
        assertThat(mapperService.fieldType("foo.metric.count.min").typeName(), equalTo("long"));
        assertThat(mapperService.fieldType("foo.metric.count.max").typeName(), equalTo("long"));
        assertNotNull(mapperService.mappingLookup().objectMappers().get("foo.metric"));
        assertNull(mapperService.mappingLookup().objectMappers().get("foo.metric.count"));
    }

    public void testSubobjectsFalseFlatPaths() throws IOException {
        MapperService mapperService = createDynamicTemplateNoSubobjects();
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("foo.metric.count", 10);
            b.field("foo.bar.baz", 10);
            b.field("foo.metric.count.min", 4);
            b.field("foo.metric.count.max", 15);
        }));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        assertNoSubobjects(mapperService);
    }

    public void testSubobjectsFalseStructuredPaths() throws IOException {
        MapperService mapperService = createDynamicTemplateNoSubobjects();
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("foo");
            {
                b.startObject("metric");
                {
                    b.field("count", 10);
                    b.field("count.min", 4);
                    b.field("count.max", 15);
                }
                b.endObject();
                b.startObject("bar");
                b.field("baz", 10);
                b.endObject();
            }
            b.endObject();
        }));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        assertNoSubobjects(mapperService);
    }

    public void testSubobjectsFalseArrayOfObjects() throws IOException {
        MapperService mapperService = createDynamicTemplateNoSubobjects();
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("foo");
            {
                b.startArray("metric");
                {
                    b.startObject();
                    {
                        b.field("count", 10);
                        b.field("count.min", 4);
                        b.field("count.max", 15);
                    }
                    b.endObject();
                    b.startObject();
                    {
                        b.field("count", 5);
                        b.field("count.min", 3);
                        b.field("count.max", 50);
                    }
                    b.endObject();
                }
                b.endArray();
                b.startObject("bar");
                b.field("baz", 10);
                b.endObject();
            }
            b.endObject();
        }));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        assertNoSubobjects(mapperService);
    }

    public void testSubobjectFalseDynamicNestedNotAllowed() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                b.startObject("nested");
                {
                    b.field("match", "object");
                    b.startObject("mapping");
                    {
                        b.field("type", "nested");
                    }
                    b.endObject();
                }
                b.endObject();
                b.endObject();
            }
            b.endArray();
            b.startObject("properties");
            b.startObject("metrics").field("type", "object").field("subobjects", false).endObject();
            b.endObject();
        }));

        DocumentParsingException err = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
            {
              "metrics.object" : [
                {}
              ]
            }
            """)));
        assertEquals("[3:5] Tried to add nested object [object] to object [metrics] which does not support subobjects", err.getMessage());
    }

    public void testRootSubobjectFalseDynamicNestedNotAllowed() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                b.startObject("nested");
                {
                    b.field("match", "object");
                    b.startObject("mapping");
                    {
                        b.field("type", "nested");
                    }
                    b.endObject();
                }
                b.endObject();
                b.endObject();
            }
            b.endArray();
            b.field("subobjects", false);
        }));

        DocumentParsingException err = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
            {
              "object" : [
                {}
              ]
            }
            """)));
        assertEquals("[3:5] Tried to add nested object [object] to object [_doc] which does not support subobjects", err.getMessage());
    }

    public void testSubobjectsFalseDocsWithGeoPointFromDynamicTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                b.startObject("location");
                {
                    b.field("match", "location.with.dots");
                    b.startObject("mapping");
                    {
                        b.field("type", "geo_point");
                    }
                    b.endObject();
                }
                b.endObject();
                b.endObject();
            }
            b.endArray();
            b.field("subobjects", false);
        }));

        ParsedDocument parsedDocument = mapper.parse(source("""
            {
              "location.with.dots" : {
                "lat": 41.12,
                "lon": -71.34
              },
              "service.time.max" : 1000
            }
            """));

        assertNotNull(parsedDocument.rootDoc().getField("service.time.max"));
        assertNotNull(parsedDocument.rootDoc().getField("location.with.dots"));
        assertNotNull(parsedDocument.dynamicMappingsUpdate());
        assertNotNull(parsedDocument.dynamicMappingsUpdate().getRoot().getMapper("service.time.max"));
        assertThat(parsedDocument.dynamicMappingsUpdate().getRoot().getMapper("location.with.dots"), instanceOf(GeoPointFieldMapper.class));
    }

    public void testDynamicSubobjectsFalseDynamicFalse() throws Exception {
        // verify that we read the dynamic value properly from the parent mapper. DocumentParser#dynamicOrDefault splits the field
        // name where dots are found, but it does that only for the parent prefix e.g. metrics.service and not for the leaf suffix time.max
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                b.startObject("metrics");
                {
                    b.field("match", "metrics");
                    b.startObject("mapping");
                    {
                        b.field("type", "object");
                        b.field("dynamic", "false");
                        b.startObject("properties");
                        {
                            b.startObject("service");
                            {
                                b.field("type", "object");
                                b.field("subobjects", false);
                                b.startObject("properties");
                                {
                                    b.startObject("time");
                                    b.field("type", "keyword");
                                    b.endObject();
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
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source("""
            {
              "metrics": {
                "service": {
                  "time" : 10,
                  "time.max" : 500
                }
              }
            }
            """));

        assertNotNull(doc.rootDoc().getField("metrics.service.time"));
        assertNull(doc.rootDoc().getField("metrics.service.time.max"));
        assertNotNull(doc.dynamicMappingsUpdate());
        ObjectMapper metrics = (ObjectMapper) doc.dynamicMappingsUpdate().getRoot().getMapper("metrics");
        assertEquals(ObjectMapper.Dynamic.FALSE, metrics.dynamic());
        assertEquals(1, metrics.mappers.size());
        ObjectMapper service = (ObjectMapper) metrics.getMapper("service");
        assertFalse(service.subobjects());
        assertEquals(1, service.mappers.size());
        assertNotNull(service.getMapper("time"));
    }

    public void testSubobjectsFalseWithInnerNestedFromDynamicTemplate() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "metric");
                        b.startObject("mapping");
                        {
                            b.field("type", "object").field("subobjects", false);
                            b.startObject("properties");
                            {
                                b.startObject("time");
                                b.field("type", "nested");
                                b.endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "Failed to parse mapping: dynamic template [test] has invalid content [{\"match\":\"metric\",\"mapping\":"
                + "{\"properties\":{\"time\":{\"type\":\"nested\"}},\"subobjects\":false,\"type\":\"object\"}}], "
                + "attempted to validate it with the following match_mapping_type: [object, string, long, double, boolean, date, binary]",
            exception.getMessage()
        );
        assertThat(exception.getRootCause(), instanceOf(MapperParsingException.class));
        assertEquals(
            "Tried to add nested object [time] to object [__dynamic__test] which does not support subobjects",
            exception.getRootCause().getMessage()
        );
    }

    public void testDynamicSubobject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            {
                b.startObject("map_artifact_identifiers");
                {
                    b.field("match_mapping_type", "object");
                    b.field("path_match", "artifacts.*");
                    b.startObject("mapping");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("identifiers");
                            {
                                b.startObject("properties");
                                b.startObject("name").field("type", "keyword").endObject();
                                b.startObject("label").field("type", "keyword").endObject();
                                b.endObject();
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
            b.endArray();
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "artifacts": {
                "dynamic1": {
                  "identifiers": {
                    "value": 100,
                    "name": "diagnostic-configuration-v1"
                  }
                },
                "dynamic2": {
                  "identifiers": {
                    "value": 500,
                    "name": "diagnostic-configuration-v2"
                  }
                }
              }
            }
            """));

        Mapping mapping = doc.dynamicMappingsUpdate();
        ObjectMapper artifacts = (ObjectMapper) mapping.getRoot().getMapper("artifacts");
        ObjectMapper dynamic1 = (ObjectMapper) artifacts.getMapper("dynamic1");
        ObjectMapper identifiers1 = (ObjectMapper) dynamic1.getMapper("identifiers");
        Mapper name1 = identifiers1.getMapper("name");
        assertThat(name1, instanceOf(KeywordFieldMapper.class));
        assertNotNull(identifiers1.getMapper("value"));
        Mapper label1 = identifiers1.getMapper("label");
        assertThat(label1, instanceOf(KeywordFieldMapper.class));

        ObjectMapper dynamic2 = (ObjectMapper) artifacts.getMapper("dynamic2");
        ObjectMapper identifiers2 = (ObjectMapper) dynamic2.getMapper("identifiers");
        Mapper name2 = identifiers2.getMapper("name");
        assertThat(name2, instanceOf(KeywordFieldMapper.class));
        assertNotNull(identifiers2.getMapper("value"));
        Mapper label2 = identifiers2.getMapper("label");
        assertThat(label2, instanceOf(KeywordFieldMapper.class));

        LuceneDocument rootDoc = doc.rootDoc();
        assertNotNull(rootDoc.getField("artifacts.dynamic1.identifiers.name"));
        assertNotNull(rootDoc.getField("artifacts.dynamic1.identifiers.value"));
        assertNotNull(rootDoc.getField("artifacts.dynamic2.identifiers.name"));
        assertNotNull(rootDoc.getField("artifacts.dynamic2.identifiers.value"));
    }

    public void testDynamicSubobjectWithInnerObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            {
                b.startObject("map_artifact_identifiers");
                {
                    b.field("match_mapping_type", "object");
                    b.field("path_match", "artifacts.*");
                    b.startObject("mapping");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("identifiers");
                            {
                                b.startObject("properties");
                                {
                                    b.startObject("name").field("type", "keyword").endObject();
                                    b.startObject("subobject");
                                    {
                                        b.startObject("properties");
                                        b.startObject("label").field("type", "keyword").endObject();
                                        b.endObject();
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
                }
                b.endObject();
            }
            b.endObject();
            b.endArray();
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "artifacts": {
                "dynamic": {
                  "identifiers": {
                    "subobject" : {
                      "label": "test",
                      "value" : 1000
                    }
                  }
                }
              }
            }
            """));

        Mapping mapping = doc.dynamicMappingsUpdate();
        ObjectMapper artifacts = (ObjectMapper) mapping.getRoot().getMapper("artifacts");
        ObjectMapper dynamic = (ObjectMapper) artifacts.getMapper("dynamic");
        ObjectMapper identifiers = (ObjectMapper) dynamic.getMapper("identifiers");
        Mapper name = identifiers.getMapper("name");
        assertThat(name, instanceOf(KeywordFieldMapper.class));
        ObjectMapper subobject = (ObjectMapper) identifiers.getMapper("subobject");
        Mapper label = subobject.getMapper("label");
        assertThat(label, instanceOf(KeywordFieldMapper.class));
        assertNotNull(subobject.getMapper("value"));

        LuceneDocument rootDoc = doc.rootDoc();
        assertNotNull(rootDoc.getField("artifacts.dynamic.identifiers.subobject.label"));
        assertNotNull(rootDoc.getField("artifacts.dynamic.identifiers.subobject.value"));
    }

    public void testDynamicSubobjectsWithFieldsAndDynamic() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            {
                b.startObject("map_artifact_identifiers");
                {
                    b.field("match_mapping_type", "object");
                    b.field("path_match", "artifacts.*");
                    b.startObject("mapping");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("identifiers");
                            {
                                b.field("dynamic", false);
                                b.startObject("properties");
                                {
                                    b.startObject("subobject");
                                    {
                                        b.field("type", "object");
                                        b.field("dynamic", true);
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
                }
                b.endObject();
            }
            b.endObject();
            b.endArray();
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "artifacts": {
                "dynamic": {
                  "identifiers": {
                    "anything": "test",
                    "subobject" : {
                      "anything" : "test"
                    }
                  }
                }
              }
            }
            """));

        Mapping mapping = doc.dynamicMappingsUpdate();
        ObjectMapper artifacts = (ObjectMapper) mapping.getRoot().getMapper("artifacts");
        ObjectMapper dynamic = (ObjectMapper) artifacts.getMapper("dynamic");
        ObjectMapper identifiers = (ObjectMapper) dynamic.getMapper("identifiers");
        assertEquals(ObjectMapper.Dynamic.FALSE, identifiers.dynamic);
        assertEquals(1, identifiers.mappers.size());
        ObjectMapper subobject = (ObjectMapper) identifiers.getMapper("subobject");
        assertEquals(ObjectMapper.Dynamic.TRUE, subobject.dynamic);
        assertNotNull(subobject.getMapper("anything"));
    }

    public void testDynamicSubobjectWithInnerObjectDocWithEmptyObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            {
                b.startObject("map_artifact_identifiers");
                {
                    b.field("match_mapping_type", "object");
                    b.field("path_match", "artifacts.*");
                    b.startObject("mapping");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("identifiers");
                            {
                                b.startObject("properties");
                                {
                                    b.startObject("name").field("type", "keyword").endObject();
                                    b.startObject("subobject");
                                    {
                                        b.startObject("properties");
                                        b.startObject("label").field("type", "keyword").endObject();
                                        b.endObject();
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
                }
                b.endObject();
            }
            b.endObject();
            b.endArray();
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "artifacts": {
                "dynamic": {
                  "identifiers": {
                  }
                }
              }
            }
            """));

        Mapping mapping = doc.dynamicMappingsUpdate();
        ObjectMapper artifacts = (ObjectMapper) mapping.getRoot().getMapper("artifacts");
        ObjectMapper dynamic = (ObjectMapper) artifacts.getMapper("dynamic");
        ObjectMapper identifiers = (ObjectMapper) dynamic.getMapper("identifiers");
        Mapper name = identifiers.getMapper("name");
        assertThat(name, instanceOf(KeywordFieldMapper.class));
        ObjectMapper subobject = (ObjectMapper) identifiers.getMapper("subobject");
        Mapper label = subobject.getMapper("label");
        assertThat(label, instanceOf(KeywordFieldMapper.class));
    }

    public void testEnabledFalseDocWithEmptyObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            {
                b.startObject("disabled_object");
                {
                    b.field("match_mapping_type", "object");
                    b.field("match", "disabled");
                    b.startObject("mapping");
                    b.field("enabled", false);
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.endArray();
        }));
        // here we provide the empty object to make sure that it gets mapped even if it does not have sub-fields defined
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "artifacts": {
                "disabled": {
                }
              }
            }
            """));

        Mapping mapping = doc.dynamicMappingsUpdate();
        ObjectMapper artifacts = (ObjectMapper) mapping.getRoot().getMapper("artifacts");
        ObjectMapper disabled = (ObjectMapper) artifacts.getMapper("disabled");
        assertFalse(disabled.enabled.value());
    }

    public void testDynamicStrictDocWithEmptyObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            {
                b.startObject("strict_object");
                {
                    b.field("match_mapping_type", "object");
                    b.field("match", "strict");
                    b.startObject("mapping");
                    b.field("dynamic", "strict");
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.endArray();
        }));
        // here we provide the empty object to make sure that it gets mapped even if it does not have sub-fields defined
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "artifacts": {
                "strict": {
                }
              }
            }
            """));

        Mapping mapping = doc.dynamicMappingsUpdate();
        ObjectMapper artifacts = (ObjectMapper) mapping.getRoot().getMapper("artifacts");
        ObjectMapper strict = (ObjectMapper) artifacts.getMapper("strict");
        assertEquals(ObjectMapper.Dynamic.STRICT, strict.dynamic());
    }

    public void testSubobjectsFalseDocWithEmptyObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            {
                b.startObject("disabled_object");
                {
                    b.field("match_mapping_type", "object");
                    b.field("match", "leaf");
                    b.startObject("mapping");
                    b.field("subobjects", false);
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.endArray();
        }));
        // here we provide the empty object to make sure that it gets mapped even if it does not have sub-fields defined
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "artifacts": {
                "leaf": {
                }
              }
            }
            """));

        Mapping mapping = doc.dynamicMappingsUpdate();
        ObjectMapper artifacts = (ObjectMapper) mapping.getRoot().getMapper("artifacts");
        ObjectMapper leaf = (ObjectMapper) artifacts.getMapper("leaf");
        assertFalse(leaf.subobjects());
    }

    public void testMatchWithArrayOfFieldNames() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match": ["long_*", "int_*"],
                      "mapping": {
                        "type": "integer"
                      }
                    }
                  }
                ]
              }
            }
            """;
        String docJson = """
            {
                "long_one": 10,
                "int_text": "12",
                "mynum": 13
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        LuceneDocument doc = parsedDoc.rootDoc();

        assertEquals(IntField.class, doc.getField("long_one").getClass());
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("long_one");
        assertNotNull(fieldMapper);
        assertEquals("integer", fieldMapper.typeName());

        assertEquals(IntField.class, doc.getField("int_text").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("int_text");
        assertNotNull(fieldMapper);
        assertEquals("integer", fieldMapper.typeName());

        assertEquals(LongField.class, doc.getField("mynum").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("mynum");
        assertNotNull(fieldMapper);
        assertEquals("long", fieldMapper.typeName());
    }

    public void testMatchAndUnmatchWithArrayOfFieldNamesMapToIpType() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match": ["ip_*", "*_ip"],
                      "unmatch": ["one*", "*two"],
                      "mapping": {
                        "type": "ip"
                      }
                    }
                  }
                ]
              }
            }
            """;
        String docJson = """
            {
                "one_ip": "will not match",
                "ip_two": "will not match",
                "three_ip": "12.12.12.12",
                "ip_four": "13.13.13.13"
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        LuceneDocument doc = parsedDoc.rootDoc();

        assertNotEquals(InetAddressPoint.class, doc.getField("one_ip").getClass());
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("one_ip");
        assertNotNull(fieldMapper);
        assertEquals("text", fieldMapper.typeName());

        assertNotEquals(InetAddressPoint.class, doc.getField("ip_two").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("ip_two");
        assertNotNull(fieldMapper);
        assertEquals("text", fieldMapper.typeName());

        assertEquals(InetAddressPoint.class, doc.getField("three_ip").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("three_ip");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        assertEquals(InetAddressPoint.class, doc.getField("ip_four").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("ip_four");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());
    }

    public void testMatchWithArrayOfFieldNamesUsingRegex() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match_pattern": "regex",
                      "match": ["^one\\\\d.*$", "^.*two", ".*xyz.*"],
                      "mapping": {
                        "type": "ip"
                      }
                    }
                  }
                ]
              }
            }
            """;
        String docJson = """
            {
                "one100_ip": "11.11.11.120",
                "iptwo": "10.10.10.10",
                "threeip": "12.12.12.12"
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        LuceneDocument doc = parsedDoc.rootDoc();

        assertEquals(InetAddressPoint.class, doc.getField("one100_ip").getClass());
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("one100_ip");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        assertEquals(InetAddressPoint.class, doc.getField("iptwo").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("iptwo");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        assertNotEquals(InetAddressPoint.class, doc.getField("threeip").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("threeip");
        assertNotNull(fieldMapper);
        assertEquals("text", fieldMapper.typeName());
    }

    public void testSimpleMatchWithArrayOfFieldNamesMixingGlobsAndRegex() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match": ["one*", ".*two$", "^xyz.*"],
                      "mapping": {
                        "type": "ip"
                      }
                    }
                  }
                ]
              }
            }
            """;
        String docJson = """
            {
                "oneip": "11.11.11.120",
                "iptwo": "10.10.10.10",
                "threeip": "12.12.12.12"
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        LuceneDocument doc = parsedDoc.rootDoc();

        assertEquals(InetAddressPoint.class, doc.getField("oneip").getClass());
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("oneip");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        // this one will not match and be an IP field because it was specified with a regex but match_pattern is implicit "simple"
        assertNotEquals(InetAddressPoint.class, doc.getField("iptwo").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("iptwo");
        assertNotNull(fieldMapper);
        assertEquals("text", fieldMapper.typeName());

        assertNotEquals(InetAddressPoint.class, doc.getField("threeip").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("threeip");
        assertNotNull(fieldMapper);
        assertEquals("text", fieldMapper.typeName());

        // patterns that appear to be regexes when using simple matching show up as warnings in the headers
        assertWarnings(
            "Pattern [.*two$] in dynamic template [test] appears to be a regular expression, "
                + "not a simple wildcard pattern. To remove this warning, set [match_pattern] in the dynamic template.",
            "Pattern [^xyz.*] in dynamic template [test] appears to be a regular expression, "
                + "not a simple wildcard pattern. To remove this warning, set [match_pattern] in the dynamic template."
        );
    }

    public void testDefaultMatchTypeWithArrayOfFieldNamesMixingGlobsAndRegexInPathMatchAndUnmatch() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "path_match": "*.*",
                      "path_unmatch": ["foo.*.bar", "[xyz]*.*", "*.iptwo", "*two.three$", "^quux.*"],
                      "mapping": {
                        "type": "ip"
                      }
                    }
                  }
                ]
              }
            }
            """;
        String docJson = """
            {
              "outer": {
                "oneip": "11.11.11.120",
                "iptwo": "10.10.10.10",
                "threeip": "12.12.12.12"
              }
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        LuceneDocument doc = parsedDoc.rootDoc();

        assertEquals(InetAddressPoint.class, doc.getField("outer.oneip").getClass());
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("outer.oneip");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        // this one will not match and be an IP field because it was specified with a regex but match_pattern is implicit "simple"
        assertNotEquals(InetAddressPoint.class, doc.getField("outer.iptwo").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("outer.iptwo");
        assertNotNull(fieldMapper);
        assertEquals("text", fieldMapper.typeName());

        assertEquals(InetAddressPoint.class, doc.getField("outer.threeip").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("outer.threeip");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        assertWarnings(
            "Pattern [[xyz]*.*] in dynamic template [test] appears to be a regular expression, "
                + "not a simple wildcard pattern. To remove this warning, set [match_pattern] in the dynamic template.",
            "Pattern [^quux.*] in dynamic template [test] appears to be a regular expression, "
                + "not a simple wildcard pattern. To remove this warning, set [match_pattern] in the dynamic template."
        );
    }

    public void testSimpleMatchTypeWithArrayOfFieldNamesMixingGlobsAndRegexInPathMatchAndUnmatch() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match_pattern": "simple",
                      "path_match": "*.*",
                      "path_unmatch": ["foo.*.bar", "[xyz]*.*", "*.iptwo", "*two.three$", "^quux.*"],
                      "mapping": {
                        "type": "ip"
                      }
                    }
                  }
                ]
              }
            }
            """;
        String docJson = """
            {
              "outer": {
                "oneip": "11.11.11.120",
                "iptwo": "10.10.10.10",
                "threeip": "12.12.12.12"
              }
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        LuceneDocument doc = parsedDoc.rootDoc();

        assertEquals(InetAddressPoint.class, doc.getField("outer.oneip").getClass());
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("outer.oneip");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        // this one will not match and be an IP field because it was specified with a regex but match_pattern is implicit "simple"
        assertNotEquals(InetAddressPoint.class, doc.getField("outer.iptwo").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("outer.iptwo");
        assertNotNull(fieldMapper);
        assertEquals("text", fieldMapper.typeName());

        assertEquals(InetAddressPoint.class, doc.getField("outer.threeip").getClass());
        fieldMapper = mapperService.documentMapper().mappers().getMapper("outer.threeip");
        assertNotNull(fieldMapper);
        assertEquals("ip", fieldMapper.typeName());

        // no warnings expected here, since match_pattern=simple was explicitly set
    }

    public void testMatchAndUnmatchWithArrayOfFieldNamesAsRuntimeFields() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match": ["*one*", "two*"],
                      "unmatch": ["*_xyz", "*foo"],
                      "runtime": {}
                    }
                  }
                ]
              }
            }
            """;
        // twothing should map to runtime field of type 'keyword' (default for runtime strings)
        // one_xyz should be excluded because of the unmatch, so be multi-field of text/keyword
        String docJson = """
            {
                "twothing": "ipsum",
                "one_xyz": "13"
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        Map<String, Object> actualDynamicMappings = XContentTestUtils.convertToMap(parsedDoc.dynamicMappingsUpdate());

        String expected = """
            {
              "_doc": {
                "runtime": {
                  "twothing": {
                    "type": "keyword"
                  }
                },
                "properties": {
                  "one_xyz": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              }
            }""";

        try (XContentParser xparser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, expected)) {
            Map<String, Object> expectedDynamicMappings = xparser.map();
            String diff = XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder(actualDynamicMappings, expectedDynamicMappings);
            assertNull("difference between expected and actual Mappings", diff);
        }
    }

    public void testMatchAndUnmatchWithArrayOfFieldNamesWithMatchMappingType() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match_mapping_type": "string",
                      "match": ["*one*", "two*"],
                      "mapping": {
                        "type": "keyword"
                      }
                    }
                  }
                ]
              }
            }
            """;
        String docJson = """
            {
                "one_bool": "true",
                "two_bool": false
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("one_bool");
        assertNotNull(fieldMapper);
        assertEquals("keyword", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("two_bool");
        assertNotNull(fieldMapper);
        // this would be keyword if we hadn't specified match_mapping_type = string
        assertEquals("boolean", fieldMapper.typeName());
    }

    public void testPathMatchWithArrayOfFieldNames() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "full_name": {
                      "path_match": ["name.*", "user.name.*"],
                      "mapping": {
                        "type":     "text",
                        "copy_to":  "full_name"
                      }
                    }
                  }
                ]
              }
            }
            """;

        String docJson1 = """
            {
              "name": {
                "first":  "John",
                "middle": "Winston",
                "last":   "Lennon"
              }
            }
            """;

        String docJson2 = """
            {
              "user": {
                "name": {
                  "first":  "Jane",
                  "midinitial": "M",
                  "last":   "Salazar"
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson1));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("name.first");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        String copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("name.middle");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("name.last");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        // test second doc with user.name.xxx
        parsedDoc = mapperService.documentMapper().parse(source(docJson2));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("user.name.first");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("user.name.midinitial");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("user.name.last");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());
    }

    public void testPathMatchAndPathUnmatchWithArrayOfFieldNames() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "full_name": {
                      "path_match":   ["name.*", "user.name.*"],
                      "path_unmatch": ["*.middle", "*.midinitial"],
                      "mapping": {
                        "type":       "text",
                        "copy_to":    "full_name"
                      }
                    }
                  }
                ]
              }
            }
            """;

        String docJson1 = """
            {
              "name": {
                "first":  "John",
                "middle": "Winston",
                "last":   "Lennon"
              }
            }
            """;

        String docJson2 = """
            {
              "user": {
                "name": {
                  "first":  "Jane",
                  "midinitial": "M",
                  "last":   "Salazar"
                }
              }
            }
            """;

        MapperService mapperService = createMapperService(mapping);
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson1));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("name.first");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        String copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("name.middle");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        assertThat(((TextFieldMapper) fieldMapper).copyTo().copyToFields(), is(empty()));
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("name.last");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        // test second doc with user.name.xxx
        parsedDoc = mapperService.documentMapper().parse(source(docJson2));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));

        fieldMapper = mapperService.documentMapper().mappers().getMapper("user.name.first");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("user.name.midinitial");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        assertThat(((TextFieldMapper) fieldMapper).copyTo().copyToFields(), is(empty()));
        assertEquals("text", fieldMapper.typeName());

        fieldMapper = mapperService.documentMapper().mappers().getMapper("user.name.last");
        assertThat(fieldMapper, instanceOf(TextFieldMapper.class));
        copyToField = ((TextFieldMapper) fieldMapper).copyTo().copyToFields().get(0);
        assertEquals("full_name", copyToField);
        assertEquals("text", fieldMapper.typeName());
    }

    public void testInvalidMatchWithArrayOfFieldNamesUsingNonStringEntries() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  {
                    "test": {
                      "match": [23.45, false],
                      "mapping": {
                        "type": "integer"
                      }
                    }
                  }
                ]
              }
            }
            """;

        // throws MapperParsingException Failed to parse mapping: [match] values must either be a string or list of strings, but was
        // [[23.45, false]]
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getMessage(), containsString("Failed to parse mapping"));
        assertThat(
            e.getCause().getMessage(),
            containsString("[match] values must either be a string or list of strings, but was [[23.45, false]]")
        );
    }

    public void testIsValidPatternForDefaultMatchType() {
        record MatchPattern(String pattern, boolean isValid) {}

        MatchPattern[] candidates = new MatchPattern[] {
            // valid match_pattern=DEFAULT entries
            new MatchPattern("", true),    // has no metacharacters
            new MatchPattern("foo", true), // has no metacharacters
            new MatchPattern("user.name", true), // regular ES dotted field, so not a "field regex"
            new MatchPattern("*foo", true),
            new MatchPattern("foo*", true),       // matches simple wildcard, so not considered an ES field regex
            new MatchPattern("foo?", true),
            new MatchPattern("foo+", true),
            new MatchPattern("a(cb)", true),
            new MatchPattern("*.*", true),        // legitimate simple pattern for dotted path name
            new MatchPattern("foo.*.bar", true),  // legitimate simple pattern for dotted path name
            new MatchPattern(".middle*", true),
            new MatchPattern("zero.one*", true),
            new MatchPattern("*two.three$", true), // doesn't compile to regex
            new MatchPattern("$^[foo", true),      // doesn't compile to regex

            // invalid match_pattern=DEFAULT entries
            new MatchPattern(".*foo", false),
            new MatchPattern("^foo", false),
            new MatchPattern("foo$", false),
            new MatchPattern("f.*oo$", false),
            new MatchPattern("a|b", false),
            new MatchPattern("a[cb]", false),
            new MatchPattern("a{1,2}", false),
            new MatchPattern("[xyz]*.*", false) };

        DynamicTemplate.MatchType matchType = DynamicTemplate.MatchType.DEFAULT;

        List<String> expectedWarnings = new ArrayList<>();
        for (MatchPattern c : candidates) {
            // should never throw an exception and will record a header warning for invalid ones
            matchType.validate(c.pattern, "my-template");

            if (c.isValid == false) {
                expectedWarnings.add(
                    "Pattern ["
                        + c.pattern
                        + "] in dynamic template [my-template] appears to be a regular expression, "
                        + "not a simple wildcard pattern. To remove this warning, set [match_pattern] in the dynamic template."
                );
            }
        }

        // DEFAULT match type isValidPattern adds header warnings, so ensure all expected ones are present
        assertWarnings(expectedWarnings.toArray(new String[0]));
    }

    public void testIsValidPatternForSimpleMatchType() {
        // match_pattern=simple does not reject any entries
        String[] candidates = new String[] {
            "*foo",
            "a(cb",
            "*.*",
            "*two.three$",
            "$^[foo",
            "zero.one*",
            ".middle*",
            "foo.*.bar",
            "foo*",
            "foo?",
            "foo+",
            "",
            "foo",
            "user.name",
            ".*foo",
            "^foo",
            "foo$",
            "f.*oo$",
            "a|b",
            "a[cb]",
            "a{1,2}",
            "[xyz]*.*" };

        DynamicTemplate.MatchType matchType = DynamicTemplate.MatchType.SIMPLE;

        for (String pattern : candidates) {
            // should not throw an Exception
            matchType.validate(pattern, "my-template");
        }

        // should set no header warnings
        assertWarnings();
    }

    public void testIsValidPatternForRegexMatchType() {
        record MatchPattern(String pattern, boolean isValid) {}

        MatchPattern[] candidates = new MatchPattern[] {
            // invalid match_pattern=REGEX entries
            new MatchPattern("*foo", false),
            new MatchPattern("*.*", false),
            new MatchPattern("*two.three$", false),
            new MatchPattern("$^[foo", false),

            // valid match_pattern=REGEX entries
            new MatchPattern("a(cb)", true),
            new MatchPattern("zero.one*", true),
            new MatchPattern(".middle*", true),
            new MatchPattern("foo.*.bar", true),
            new MatchPattern("foo*", true),
            new MatchPattern("foo?", true),
            new MatchPattern("foo+", true),
            new MatchPattern("", true),
            new MatchPattern("foo", true),
            new MatchPattern("user.name", true),
            new MatchPattern(".*foo", true),
            new MatchPattern("^foo", true),
            new MatchPattern("foo$", true),
            new MatchPattern("f.*oo$", true),
            new MatchPattern("a|b", true),
            new MatchPattern("a[cb]", true),
            new MatchPattern("a{1,2}", true),
            new MatchPattern("[xyz]*.*", true) };

        DynamicTemplate.MatchType matchType = DynamicTemplate.MatchType.REGEX;

        for (MatchPattern c : candidates) {
            if (c.isValid) {
                // should not throw an Exception
                matchType.validate(c.pattern, "my-template");
            } else {
                Exception e = expectThrows(
                    MapperParsingException.class,
                    "pattern tested: " + c.pattern,
                    () -> matchType.validate(c.pattern, "my-template")
                );
            }
        }
    }
}
