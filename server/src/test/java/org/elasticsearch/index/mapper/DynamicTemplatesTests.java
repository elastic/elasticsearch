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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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
        assertFalse(mapperService.fieldType("s").isSearchable());

        assertThat(mapperService.fieldType("l"), notNullValue());
        assertTrue(mapperService.fieldType("l").isSearchable());
    }

    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-mapping.json");
        MapperService mapperService = createMapperService(mapping);
        String docJson = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-data.json");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(docJson));

        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        Document doc = parsedDoc.rootDoc();

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
        Document doc = parsedDoc.rootDoc();

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
            Version version = VersionUtils.randomCompatibleVersion(random(), Version.V_7_0_0);
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
            assertWarnings(
                "dynamic template [test] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{\"badparam\":false}}], " +
                "attempted to validate it with the following match_mapping_type: [string], last error: " +
                "[unknown parameter [badparam] on mapper [__dynamic__test] of type [null]]");

            mapper.parse(source(b -> b.field("field", "foo")));
            assertWarnings("Parameter [badparam] is used in a dynamic template mapping and has no effect on type [null]. " +
                    "Usage will result in an error in future major versions and should be removed.");
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
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
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
            .endObject());
        MapperService mapperService = createMapperService(mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .endObject()
            .endObject());
        merge(mapperService, mapping2);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .field("dynamic_templates", Collections.emptyList())
            .endObject()
            .endObject());
        merge(mapperService, mapping3);
        assertEquals(mapping3, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDynamicTemplatesForIndexTemplate() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
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
            .endObject());
        MapperService mapperService = createMapperService(Version.CURRENT, Settings.EMPTY, () -> true);
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MapperService.MergeReason.INDEX_TEMPLATE);

        // There should be no update if templates are not set.
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject());
        DocumentMapper mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(mapping), MapperService.MergeReason.INDEX_TEMPLATE);

        DynamicTemplate[] templates = mapper.mapping().getRoot().dynamicTemplates();
        assertEquals(2, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second", templates[1].pathMatch());

        // Dynamic templates should be appended and deduplicated.
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
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
            .endObject());
        mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping),
            MapperService.MergeReason.INDEX_TEMPLATE);

        templates = mapper.mapping().getRoot().dynamicTemplates();
        assertEquals(3, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second_updated", templates[1].pathMatch());
        assertEquals("third_template", templates[2].name());
        assertEquals("third", templates[2].pathMatch());
    }

    public void testIllegalDynamicTemplates() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("dynamic_templates")
            .endObject()
            .endObject()
            .endObject());

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
        assertThat(e.getRootCause().getMessage(),
            equalTo("unknown parameter [foo] on mapper [__dynamic__my_template] of type [keyword]"));
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
        assertEquals("Failed to parse mapping: dynamic template [my_template] has invalid content [" +
            "{\"match_mapping_type\":\"string\",\"runtime\":{\"foo\":\"bar\",\"type\":\"keyword\"}}], " +
            "attempted to validate it with the following match_mapping_type: [string]", e.getMessage());
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
            assertThat(e.getRootCause().getMessage(),
                equalTo("unknown parameter [foo] on mapper [__dynamic__my_template] of type [binary]"));
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
        assertThat(e.getMessage(), containsString("attempted to validate it with the following match_mapping_type: " +
            "[string, long, double, boolean, date]"));
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
        Version createdVersion = randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_7_0);
        MapperService mapperService = createMapperService(createdVersion, mapping);
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"type\":\"string\""));
        assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":" +
            "\"string\"}}], attempted to validate it with the following match_mapping_type: [string], " +
            "last error: [No mapper found for type [string]]");
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
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1",
            new BytesArray("{\"foo\": \"41.12,-71.34\", \"bar\": \"41.12,-71.34\"}"), XContentType.JSON, null, Map.of("foo", "geo_point")));
        assertThat(doc.rootDoc().getFields("foo"), arrayWithSize(2));
        assertThat(doc.rootDoc().getFields("bar"), arrayWithSize(1));
    }
}
