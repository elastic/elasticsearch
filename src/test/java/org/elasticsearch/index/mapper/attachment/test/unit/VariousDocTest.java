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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.elasticsearch.index.mapper.attachment.test.MapperTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.attachment.AttachmentMapper.FieldNames.*;
import static org.elasticsearch.plugin.mapper.attachments.tika.TikaInstance.tika;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

/**
 * Test for different documents
 */
public class VariousDocTest extends AttachmentUnitTestCase {

    protected DocumentMapper docMapper;

    @Before
    public void createMapper() throws IOException {
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperParser(ImmutableSettings.builder().build());
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/various-doc/test-mapping.json");
        docMapper = mapperParser.parse(mapping);
    }

    /**
     * Test for https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/104
     */
    @Test
    public void testWordDocxDocument104() throws Exception {
        testTika("issue-104.docx", false);
        testMapper("issue-104.docx", false);
    }

    /**
     * Test for encrypted PDF
     */
    @Test
    public void testEncryptedPDFDocument() throws Exception {
        testTika("encrypted.pdf", true);
        // TODO Remove when this will be fixed in Tika. See https://issues.apache.org/jira/browse/TIKA-1548
        System.clearProperty("sun.font.fontmanager");
        testMapper("encrypted.pdf", true);
    }

    /**
     * Test for HTML
     */
    @Test
    public void testHtmlDocument() throws Exception {
        testTika("htmlWithEmptyDateMeta.html", false);
        testMapper("htmlWithEmptyDateMeta.html", false);
    }

    /**
     * Test for XHTML
     */
    @Test
    public void testXHtmlDocument() throws Exception {
        testTika("testXHTML.html", false);
        testMapper("testXHTML.html", false);
    }

    /**
     * Test for TXT
     */
    @Test
    public void testTxtDocument() throws Exception {
        testTika("text-in-english.txt", false);
        testMapper("text-in-english.txt", false);
    }

    /**
     * Test for ASCIIDOC
     * Not yet supported by Tika: https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/29
     */
    @Test
    public void testAsciidocDocument() throws Exception {
        testTika("asciidoc.asciidoc", false);
        testMapper("asciidoc.asciidoc", false);
    }

    protected void testTika(String filename, boolean errorExpected) {
        Tika tika = tika();
        assumeTrue("Tika has been disabled. Ignoring test...", tika != null);

        try (InputStream is = VariousDocTest.class.getResourceAsStream("/org/elasticsearch/index/mapper/attachment/test/sample-files/" + filename)) {
            String parsedContent = tika.parseToString(is);
            assertThat(parsedContent, not(isEmptyOrNullString()));
            logger.debug("extracted content: {}", parsedContent);
        } catch (Throwable e) {
            if (!errorExpected) {
                fail("exception caught: " + e.getMessage());
            }
        }
    }

    protected void testMapper(String filename, boolean errorExpected) throws IOException {
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/" + filename);

        BytesReference json = jsonBuilder()
                .startObject()
                    .field("_id", 1)
                    .startObject("file")
                        .field("_name", filename)
                        .field("_content", html)
                    .endObject()
                .endObject().bytes();

        ParseContext.Document doc =  docMapper.parse(json).rootDoc();
        if (!errorExpected) {
            assertThat(doc.get(docMapper.mappers().smartName("file").mapper().names().indexName()), not(isEmptyOrNullString()));
            logger.debug("-> extracted content: {}", doc.get(docMapper.mappers().smartName("file").mapper().names().indexName()));
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
        logger.debug("- [{}]: [{}]", field, doc.get(docMapper.mappers().smartName("file." + field).mapper().names().indexName()));
    }
}
