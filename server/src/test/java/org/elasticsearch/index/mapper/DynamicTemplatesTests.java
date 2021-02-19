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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.ParseContext.Document;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

    public void testMatchTypeRuntimeNoPlugin() throws Exception {
        XContentBuilder topMapping = topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("runtime").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        });

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping));
        assertEquals("Failed to parse mapping: dynamic template [test] has invalid content [" +
            "{\"match_mapping_type\":\"string\",\"runtime\":{}}], " +
            "attempted to validate it with the following match_mapping_type: [string]", e.getMessage());
        assertEquals("No runtime field found for type [keyword]", e.getRootCause().getMessage());
    }

    public void testMatchAllTypesRuntimeNoPlugin() throws Exception {
        XContentBuilder topMapping = topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        if (randomBoolean()) {
                            b.field("match_mapping_type", "*");
                        } else {
                            b.field("match", "field");
                        }
                        b.startObject("runtime").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        });

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping));
        assertThat(e.getMessage(), containsString("Failed to parse mapping: dynamic template [test] has invalid content ["));
        assertThat(e.getMessage(), containsString("attempted to validate it with the following match_mapping_type: " +
            "[string, long, double, boolean, date]"));
        assertEquals("No runtime field found for type [date]", e.getRootCause().getMessage());
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
}
