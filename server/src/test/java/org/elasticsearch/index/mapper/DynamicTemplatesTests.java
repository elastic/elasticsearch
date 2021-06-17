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
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;

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
}
