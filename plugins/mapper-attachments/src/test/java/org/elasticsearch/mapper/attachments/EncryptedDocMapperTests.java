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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Test for https://github.com/elastic/elasticsearch-mapper-attachments/issues/18
 * Note that we have converted /org/elasticsearch/index/mapper/xcontent/testContentLength.txt
 * to a /org/elasticsearch/index/mapper/xcontent/encrypted.pdf with password `12345678`.
 */
public class EncryptedDocMapperTests extends AttachmentUnitTestCase {

    public void testMultipleDocsEncryptedLast() throws IOException {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/encrypted/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse("person", new CompressedXContent(mapping));
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/htmlWithValidDateMeta.html");
        byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/encrypted.pdf");

        BytesReference json = jsonBuilder()
                .startObject()
                    .field("file1", html)
                    .field("file2", pdf)
                .endObject().bytes();

        ParseContext.Document doc =  docMapper.parse("person", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.mappers().getMapper("file1.content").fieldType().name()), containsString("World"));
        assertThat(doc.get(docMapper.mappers().getMapper("file1.title").fieldType().name()), equalTo("Hello"));
        assertThat(doc.get(docMapper.mappers().getMapper("file1.author").fieldType().name()), equalTo("kimchy"));
        assertThat(doc.get(docMapper.mappers().getMapper("file1.keywords").fieldType().name()), equalTo("elasticsearch,cool,bonsai"));
        assertThat(doc.get(docMapper.mappers().getMapper("file1.content_type").fieldType().name()), startsWith("text/html;"));
        assertThat(doc.getField(docMapper.mappers().getMapper("file1.content_length").fieldType().name()).numericValue().longValue(), greaterThan(0L));

        assertThat(doc.get(docMapper.mappers().getMapper("file2").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file2.title").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file2.author").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file2.keywords").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file2.content_type").fieldType().name()), nullValue());
        assertThat(doc.getField(docMapper.mappers().getMapper("file2.content_length").fieldType().name()), nullValue());
    }

    public void testMultipleDocsEncryptedFirst() throws IOException {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/encrypted/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse("person", new CompressedXContent(mapping));
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/htmlWithValidDateMeta.html");
        byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/encrypted.pdf");

        BytesReference json = jsonBuilder()
                .startObject()
                .field("file1", pdf)
                .field("file2", html)
                .endObject().bytes();

        ParseContext.Document doc =  docMapper.parse("person", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.mappers().getMapper("file1").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file1.title").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file1.author").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file1.keywords").fieldType().name()), nullValue());
        assertThat(doc.get(docMapper.mappers().getMapper("file1.content_type").fieldType().name()), nullValue());
        assertThat(doc.getField(docMapper.mappers().getMapper("file1.content_length").fieldType().name()), nullValue());

        assertThat(doc.get(docMapper.mappers().getMapper("file2.content").fieldType().name()), containsString("World"));
        assertThat(doc.get(docMapper.mappers().getMapper("file2.title").fieldType().name()), equalTo("Hello"));
        assertThat(doc.get(docMapper.mappers().getMapper("file2.author").fieldType().name()), equalTo("kimchy"));
        assertThat(doc.get(docMapper.mappers().getMapper("file2.keywords").fieldType().name()), equalTo("elasticsearch,cool,bonsai"));
        assertThat(doc.get(docMapper.mappers().getMapper("file2.content_type").fieldType().name()), startsWith("text/html;"));
        assertThat(doc.getField(docMapper.mappers().getMapper("file2.content_length").fieldType().name()).numericValue().longValue(), greaterThan(0L));
    }

    public void testMultipleDocsEncryptedNotIgnoringErrors() throws IOException {
        try {
            DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(createTempDir(),
                Settings.builder().put("index.mapping.attachment.ignore_errors", false).build(),
                getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();

            String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/encrypted/test-mapping.json");
            DocumentMapper docMapper = mapperParser.parse("person", new CompressedXContent(mapping));
            byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/htmlWithValidDateMeta.html");
            byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/encrypted.pdf");

            BytesReference json = jsonBuilder()
                .startObject()
                .field("file1", pdf)
                .field("file2", html)
                .endObject().bytes();

            docMapper.parse("person", "person", "1", json);
            fail("Expected doc parsing exception");
        } catch (MapperParsingException e) {
            if (e.getMessage() == null || e.getMessage().contains("is encrypted") == false) {
                // wrong exception
                throw e;
            }
        }
    }

}
