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

package org.elasticsearch.index.mapper.attachment.test.unit;

import org.apache.tika.Tika;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.elasticsearch.index.mapper.attachment.test.MapperTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.attachment.AttachmentMapper.FieldNames.*;
import static org.elasticsearch.plugin.mapper.attachments.tika.TikaInstance.tika;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

/**
 * Test for different documents
 */
public class VariousDocTest extends AttachmentUnitTestCase {

    protected DocumentMapper docMapper;

    @Before
    public void createMapper() throws IOException {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperParser(createTempDir());
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/various-doc/test-mapping.json");
        docMapper = mapperParser.parse(mapping);
    }

    /**
     * Test for https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/104
     */
    @Test
    public void testWordDocxDocument104() throws Exception {
        assertParseable("issue-104.docx");
        testMapper("issue-104.docx", false);
    }

    /**
     * Test for encrypted PDF
     */
    @Test
    public void testEncryptedPDFDocument() throws Exception {
        assertException("encrypted.pdf");
        // TODO Remove when this will be fixed in Tika. See https://issues.apache.org/jira/browse/TIKA-1548
        System.clearProperty("sun.font.fontmanager");
        testMapper("encrypted.pdf", true);
    }

    /**
     * Test for HTML
     */
    @Test
    public void testHtmlDocument() throws Exception {
        assertParseable("htmlWithEmptyDateMeta.html");
        testMapper("htmlWithEmptyDateMeta.html", false);
    }

    /**
     * Test for XHTML
     */
    @Test
    public void testXHtmlDocument() throws Exception {
        assertParseable("testXHTML.html");
        testMapper("testXHTML.html", false);
    }

    /**
     * Test for TXT
     */
    @Test
    public void testTxtDocument() throws Exception {
        assertParseable("text-in-english.txt");
        testMapper("text-in-english.txt", false);
    }

    /**
     * Test for ASCIIDOC
     * Not yet supported by Tika: https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/29
     */
    @Test
    public void testAsciidocDocument() throws Exception {
        assertParseable("asciidoc.asciidoc");
        testMapper("asciidoc.asciidoc", false);
    }

    void assertException(String filename) {
        Tika tika = tika();
        assumeTrue("Tika has been disabled. Ignoring test...", tika != null);

        try (InputStream is = VariousDocTest.class.getResourceAsStream("/org/elasticsearch/index/mapper/attachment/test/sample-files/" + filename)) {
            tika.parseToString(is);
            fail("expected exception");
        } catch (Exception e) {
            // expected. TODO: check message
        }
    }

    protected void assertParseable(String filename) throws Exception {
        Tika tika = tika();
        assumeTrue("Tika has been disabled. Ignoring test...", tika != null);

        try (InputStream is = VariousDocTest.class.getResourceAsStream("/org/elasticsearch/index/mapper/attachment/test/sample-files/" + filename)) {
            String parsedContent = tika.parseToString(is);
            assertThat(parsedContent, not(isEmptyOrNullString()));
            logger.debug("extracted content: {}", parsedContent);
        }
    }

    protected void testMapper(String filename, boolean errorExpected) throws IOException {
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/" + filename);

        BytesReference json = jsonBuilder()
                .startObject()
                    .startObject("file")
                        .field("_name", filename)
                        .field("_content", html)
                    .endObject()
                .endObject().bytes();

        ParseContext.Document doc =  docMapper.parse("person", "person", "1", json).rootDoc();
        if (!errorExpected) {
            assertThat(doc.get(docMapper.mappers().getMapper("file.content").fieldType().names().indexName()), not(isEmptyOrNullString()));
            logger.debug("-> extracted content: {}", doc.get(docMapper.mappers().getMapper("file").fieldType().names().indexName()));
            logger.debug("-> extracted metadata:");
            printMetadataContent(doc, AUTHOR);
            printMetadataContent(doc, CONTENT_LENGTH);
            printMetadataContent(doc, CONTENT_TYPE);
            printMetadataContent(doc, DATE);
            printMetadataContent(doc, KEYWORDS);
            printMetadataContent(doc, LANGUAGE);
            printMetadataContent(doc, NAME);
            printMetadataContent(doc, TITLE);
        }
    }

    private void printMetadataContent(ParseContext.Document doc, String field) {
        logger.debug("- [{}]: [{}]", field, doc.get(docMapper.mappers().getMapper("file." + field).fieldType().names().indexName()));
    }
}
