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

package org.elasticsearch.mapper.attachments;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleAttachmentMapperTests extends AttachmentUnitTestCase {

    public void testSimpleMappings() throws Exception {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/simple/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testXHTML.html");

        BytesReference json = jsonBuilder().startObject().field("file", html).endObject().bytes();
        ParseContext.Document doc = docMapper.parse("person", "person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("file.content_type").fieldType().names().indexName()), startsWith("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.title").fieldType().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.content").fieldType().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));

        // re-parse it
        String builtMapping = docMapper.mappingSource().string();
        docMapper = mapperParser.parse(builtMapping);

        json = jsonBuilder().startObject().field("file", html).endObject().bytes();

        doc = docMapper.parse("person", "person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("file.content_type").fieldType().names().indexName()), startsWith("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.title").fieldType().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.content").fieldType().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));
    }

    public void testContentBackcompat() throws Exception {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(createTempDir(),
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build(),
            getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/simple/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testXHTML.html");

        BytesReference json = jsonBuilder().startObject().field("file", html).endObject().bytes();

        ParseContext.Document doc = docMapper.parse("person", "person", "1", json).rootDoc();
        assertThat(doc.get("file"), containsString("This document tests the ability of Apache Tika to extract content"));
    }

    /**
     * test for https://github.com/elastic/elasticsearch-mapper-attachments/issues/179
     */
    public void testSimpleMappingsWithAllFields() throws Exception {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/simple/test-mapping-all-fields.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testXHTML.html");

        BytesReference json = jsonBuilder().startObject().field("file", html).endObject().bytes();
        ParseContext.Document doc = docMapper.parse("person", "person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("file.content_type").fieldType().names().indexName()), startsWith("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.title").fieldType().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.content").fieldType().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));

        // re-parse it
        String builtMapping = docMapper.mappingSource().string();
        docMapper = mapperParser.parse(builtMapping);

        json = jsonBuilder().startObject().field("file", html).endObject().bytes();

        doc = docMapper.parse("person", "person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("file.content_type").fieldType().names().indexName()), startsWith("application/xhtml+xml"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.title").fieldType().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.content").fieldType().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));
    }

    /**
     * See issue https://github.com/elastic/elasticsearch-mapper-attachments/issues/169
     * Mapping should not contain field names with dot.
     */
    public void testMapperErrorWithDotTwoLevels169() throws Exception {
        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject()
                .startObject("mail")
                .startObject("properties")
                .startObject("attachments")
                .startObject("properties")
                .startObject("innerfield")
                .field("type", "attachment")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        byte[] mapping = mappingBuilder.bytes().toBytes();
        MapperService mapperService = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper());
        DocumentMapper docMapper = mapperService.parse("mail", new CompressedXContent(mapping), true);
        // this should not throw an exception
        mapperService.parse("mail", new CompressedXContent(docMapper.mapping().toString()), true);
        // the mapping may not contain a field name with a dot
        assertFalse(docMapper.mapping().toString().contains("."));
    }

}
