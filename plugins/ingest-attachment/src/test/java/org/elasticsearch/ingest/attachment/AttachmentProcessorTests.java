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

package org.elasticsearch.ingest.attachment;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

public class AttachmentProcessorTests extends ESTestCase {

    private AttachmentProcessor processor;

    @Before
    public void createStandardProcessor() throws IOException {
        // We test the default behavior which is extracting all metadata but the raw_metadata
        EnumSet<AttachmentProcessor.Property> properties = EnumSet.allOf(AttachmentProcessor.Property.class);
        properties.remove(AttachmentProcessor.Property.RAW_METADATA);
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field",
            "target_field", properties, 10000, false);
    }

    public void testEnglishTextDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-in-english.txt", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("content"), is("\"God Save the Queen\" (alternatively \"God Save the King\""));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
    }

    public void testHtmlDocumentWithRandomFields() throws Exception {
        //date is not present in the html doc
        ArrayList<AttachmentProcessor.Property> fieldsList = new ArrayList<>(EnumSet.complementOf(EnumSet.of
            (AttachmentProcessor.Property.DATE)));
        Set<AttachmentProcessor.Property> selectedProperties = new HashSet<>();

        int numFields = randomIntBetween(1, fieldsList.size());
        String[] selectedFieldNames = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            AttachmentProcessor.Property property;
            do {
                property = randomFrom(fieldsList);
            } while (selectedProperties.add(property) == false);

            selectedFieldNames[i] = property.toLowerCase();
        }
        if (randomBoolean()) {
            selectedProperties.add(AttachmentProcessor.Property.DATE);
        }
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field",
            "target_field", selectedProperties, 10000, false);

        Map<String, Object> attachmentData = parseDocument("htmlWithEmptyDateMeta.html", processor);
        assertThat(attachmentData.keySet(), hasSize(selectedFieldNames.length));
        assertThat(attachmentData.keySet(), containsInAnyOrder(selectedFieldNames));
    }

    public void testFrenchTextDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-in-french.txt", processor);

        assertThat(attachmentData.keySet(), hasItem("language"));
        assertThat(attachmentData.get("language"), is("fr"));
    }

    public void testUnknownLanguageDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-gibberish.txt", processor);

        assertThat(attachmentData.keySet(), hasItem("language"));
        // lt seems some standard for not detected
        assertThat(attachmentData.get("language"), is("lt"));
    }

    public void testEmptyTextDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-empty.txt", processor);
        assertThat(attachmentData.keySet(), not(hasItem("language")));
    }

    public void testWordDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-104.docx", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("content", "language", "date", "author", "content_type",
            "content_length"));
        assertThat(attachmentData.get("content"), is(notNullValue()));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("date"), is("2012-10-12T11:17:00Z"));
        assertThat(attachmentData.get("author"), is("Windows User"));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
        assertThat(attachmentData.get("content_type").toString(),
            is("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
    }

    public void testPdf() throws Exception {
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);
        assertThat(attachmentData.get("content"),
                is("This is a test, with umlauts, from München\n\nAlso contains newlines for testing.\n\nAnd one more."));
        assertThat(attachmentData.get("content_type").toString(), is("application/pdf"));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
    }

    public void testEncryptedPdf() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parseDocument("encrypted.pdf", processor));
        assertThat(e.getDetailedMessage(), containsString("document is encrypted"));
    }

    public void testHtmlDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("htmlWithEmptyDateMeta.html", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "author", "keywords", "title", "content_type",
            "content_length"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("content"), is(notNullValue()));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
        assertThat(attachmentData.get("author"), is("kimchy"));
        assertThat(attachmentData.get("keywords"), is("elasticsearch,cool,bonsai"));
        assertThat(attachmentData.get("title"), is("Hello"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/html"));
    }

    public void testXHtmlDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("testXHTML.html", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "author", "title", "content_type", "content_length"));
        assertThat(attachmentData.get("content_type").toString(), containsString("application/xhtml+xml"));
    }

    public void testEpubDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("testEPUB.epub", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "author", "title", "content_type", "content_length",
            "date", "keywords"));
        assertThat(attachmentData.get("content_type").toString(), containsString("application/epub+zip"));
    }

    // no real detection, just rudimentary
    public void testAsciidocDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("asciidoc.asciidoc", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content_type", "content", "content_length"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
    }

    public void testParseAsBytesArray() throws Exception {
        String path = "/org/elasticsearch/ingest/attachment/test/sample-files/text-in-english.txt";
        byte[] bytes;
        try (InputStream is = AttachmentProcessorTests.class.getResourceAsStream(path)) {
            bytes = IOUtils.toByteArray(is);
        }

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", bytes);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> attachmentData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("content"), is("\"God Save the Queen\" (alternatively \"God Save the King\""));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget", null, 10, true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget", null, 10, true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget", null, 10, false);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot parse."));
    }

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget", null, 10, false);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    public void testRawMetadataFromWordDocument() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field",
            "target_field", EnumSet.of(AttachmentProcessor.Property.RAW_METADATA), 10000, false);

        Map<String, Object> attachmentData = parseDocument("issue-104.docx", processor);

        assertThat(attachmentData.keySet(), contains("raw_metadata"));
        assertThat(attachmentData.get("raw_metadata"), is(instanceOf(Map.class)));
        @SuppressWarnings("unchecked")
        Map<String, Object> rawMetadata = (Map<String, Object>) attachmentData.get("raw_metadata");
        assertThat(rawMetadata.keySet(), containsInAnyOrder("date", "cp:revision", "Total-Time", "extended-properties:AppVersion",
            "meta:paragraph-count", "meta:word-count", "dc:creator", "extended-properties:Company", "Word-Count", "dcterms:created",
            "meta:line-count", "Last-Modified", "dcterms:modified", "Last-Save-Date", "meta:character-count", "Template",
            "Line-Count", "Paragraph-Count", "meta:save-date", "meta:character-count-with-spaces", "Application-Name",
            "extended-properties:TotalTime", "modified", "Content-Type", "X-Parsed-By", "creator", "meta:author",
            "meta:creation-date", "extended-properties:Application", "meta:last-author", "Creation-Date", "xmpTPg:NPages",
            "Character-Count-With-Spaces", "Last-Author", "Character Count", "Page-Count", "Revision-Number",
            "Application-Version", "extended-properties:Template", "Author", "publisher", "meta:page-count", "dc:publisher"));

        // An easy way to generate all metadata assertions is by running
        /*for (Map.Entry<String, Object> entry : rawMetadata.entrySet()) {
            logger.info("assertThat(rawMetadata, hasEntry(\"{}\", \"{}\"));", entry.getKey(), entry.getValue());
        }*/

        assertThat(rawMetadata, hasEntry("date", "2015-02-20T11:36:00Z"));
        assertThat(rawMetadata, hasEntry("cp:revision", "22"));
        assertThat(rawMetadata, hasEntry("Total-Time", "6"));
        assertThat(rawMetadata, hasEntry("extended-properties:AppVersion", "15.0000"));
        assertThat(rawMetadata, hasEntry("meta:paragraph-count", "1"));
        assertThat(rawMetadata, hasEntry("meta:word-count", "15"));
        assertThat(rawMetadata, hasEntry("dc:creator", "Windows User"));
        assertThat(rawMetadata, hasEntry("extended-properties:Company", "JDI"));
        assertThat(rawMetadata, hasEntry("Word-Count", "15"));
        assertThat(rawMetadata, hasEntry("dcterms:created", "2012-10-12T11:17:00Z"));
        assertThat(rawMetadata, hasEntry("meta:line-count", "1"));
        assertThat(rawMetadata, hasEntry("Last-Modified", "2015-02-20T11:36:00Z"));
        assertThat(rawMetadata, hasEntry("dcterms:modified", "2015-02-20T11:36:00Z"));
        assertThat(rawMetadata, hasEntry("Last-Save-Date", "2015-02-20T11:36:00Z"));
        assertThat(rawMetadata, hasEntry("meta:character-count", "92"));
        assertThat(rawMetadata, hasEntry("Template", "Normal.dotm"));
        assertThat(rawMetadata, hasEntry("Line-Count", "1"));
        assertThat(rawMetadata, hasEntry("Paragraph-Count", "1"));
        assertThat(rawMetadata, hasEntry("meta:save-date", "2015-02-20T11:36:00Z"));
        assertThat(rawMetadata, hasEntry("meta:character-count-with-spaces", "106"));
        assertThat(rawMetadata, hasEntry("Application-Name", "Microsoft Office Word"));
        assertThat(rawMetadata, hasEntry("extended-properties:TotalTime", "6"));
        assertThat(rawMetadata, hasEntry("modified", "2015-02-20T11:36:00Z"));
        assertThat(rawMetadata, hasEntry("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
        assertThat(rawMetadata, hasEntry("X-Parsed-By", "org.apache.tika.parser.microsoft.ooxml.OOXMLParser"));
        assertThat(rawMetadata, hasEntry("creator", "Windows User"));
        assertThat(rawMetadata, hasEntry("meta:author", "Windows User"));
        assertThat(rawMetadata, hasEntry("meta:creation-date", "2012-10-12T11:17:00Z"));
        assertThat(rawMetadata, hasEntry("extended-properties:Application", "Microsoft Office Word"));
        assertThat(rawMetadata, hasEntry("meta:last-author", "Luka Lampret"));
        assertThat(rawMetadata, hasEntry("Creation-Date", "2012-10-12T11:17:00Z"));
        assertThat(rawMetadata, hasEntry("xmpTPg:NPages", "1"));
        assertThat(rawMetadata, hasEntry("Character-Count-With-Spaces", "106"));
        assertThat(rawMetadata, hasEntry("Last-Author", "Luka Lampret"));
        assertThat(rawMetadata, hasEntry("Character Count", "92"));
        assertThat(rawMetadata, hasEntry("Page-Count", "1"));
        assertThat(rawMetadata, hasEntry("Revision-Number", "22"));
        assertThat(rawMetadata, hasEntry("Application-Version", "15.0000"));
        assertThat(rawMetadata, hasEntry("extended-properties:Template", "Normal.dotm"));
        assertThat(rawMetadata, hasEntry("Author", "Windows User"));
        assertThat(rawMetadata, hasEntry("publisher", "JDI"));
        assertThat(rawMetadata, hasEntry("meta:page-count", "1"));
        assertThat(rawMetadata, hasEntry("dc:publisher", "JDI"));
    }

    public void testRawMetadataFromPdf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field",
            "target_field", EnumSet.of(AttachmentProcessor.Property.RAW_METADATA), 10000, false);
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);
        assertThat(attachmentData.keySet(), contains("raw_metadata"));
        assertThat(attachmentData.get("raw_metadata"), is(instanceOf(Map.class)));
        @SuppressWarnings("unchecked")
        Map<String, Object> rawMetadata = (Map<String, Object>) attachmentData.get("raw_metadata");

        assertThat(rawMetadata.keySet(), containsInAnyOrder("pdf:PDFVersion", "X-Parsed-By", "xmp:CreatorTool",
            "access_permission:modify_annotations", "access_permission:can_print_degraded", "meta:creation-date", "created",
            "access_permission:extract_for_accessibility", "access_permission:assemble_document", "xmpTPg:NPages", "Creation-Date",
            "dcterms:created", "dc:format", "access_permission:extract_content", "access_permission:can_print",
            "pdf:docinfo:creator_tool", "access_permission:fill_in_form", "pdf:encrypted", "producer", "access_permission:can_modify",
            "pdf:docinfo:producer", "pdf:docinfo:created", "Content-Type"));

        // "created" is different depending on the JVM Locale. We skip testing its content
        assertThat(rawMetadata, hasEntry("pdf:PDFVersion", "1.4"));
        assertThat(rawMetadata, hasEntry("X-Parsed-By", "org.apache.tika.parser.pdf.PDFParser"));
        assertThat(rawMetadata, hasEntry("xmp:CreatorTool", "Writer"));
        assertThat(rawMetadata, hasEntry("access_permission:modify_annotations", "true"));
        assertThat(rawMetadata, hasEntry("access_permission:can_print_degraded", "true"));
        assertThat(rawMetadata, hasEntry("meta:creation-date", "2016-09-30T13:19:58Z"));
        assertThat(rawMetadata, hasEntry("access_permission:extract_for_accessibility", "true"));
        assertThat(rawMetadata, hasEntry("access_permission:assemble_document", "true"));
        assertThat(rawMetadata, hasEntry("xmpTPg:NPages", "1"));
        assertThat(rawMetadata, hasEntry("Creation-Date", "2016-09-30T13:19:58Z"));
        assertThat(rawMetadata, hasEntry("dcterms:created", "2016-09-30T13:19:58Z"));
        assertThat(rawMetadata, hasEntry("dc:format", "application/pdf; version=1.4"));
        assertThat(rawMetadata, hasEntry("access_permission:extract_content", "true"));
        assertThat(rawMetadata, hasEntry("access_permission:can_print", "true"));
        assertThat(rawMetadata, hasEntry("pdf:docinfo:creator_tool", "Writer"));
        assertThat(rawMetadata, hasEntry("access_permission:fill_in_form", "true"));
        assertThat(rawMetadata, hasEntry("pdf:encrypted", "false"));
        assertThat(rawMetadata, hasEntry("producer", "LibreOffice 5.2"));
        assertThat(rawMetadata, hasEntry("access_permission:can_modify", "true"));
        assertThat(rawMetadata, hasEntry("pdf:docinfo:producer", "LibreOffice 5.2"));
        assertThat(rawMetadata, hasEntry("pdf:docinfo:created", "2016-09-30T13:19:58Z"));
        assertThat(rawMetadata, hasEntry("Content-Type", "application/pdf"));
    }

    public void testRawMetadataFromRtf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field",
            "target_field", EnumSet.of(AttachmentProcessor.Property.RAW_METADATA), 10000, false);
        Map<String, Object> attachmentData =
            parseBase64Document("e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0=", processor);
        assertThat(attachmentData.keySet(), contains("raw_metadata"));
        assertThat(attachmentData.get("raw_metadata"), is(instanceOf(Map.class)));
        @SuppressWarnings("unchecked")
        Map<String, Object> rawMetadata = (Map<String, Object>) attachmentData.get("raw_metadata");

        assertThat(rawMetadata.keySet(), containsInAnyOrder("X-Parsed-By", "Content-Type"));

        assertThat(rawMetadata, hasEntry("X-Parsed-By", "org.apache.tika.parser.rtf.RTFParser"));
        assertThat(rawMetadata, hasEntry("Content-Type", "application/rtf"));
    }

    private Map<String, Object> parseDocument(String file, AttachmentProcessor processor) throws Exception {
        return parseBase64Document(getAsBase64(file), processor);
    }

    // Adding this method to more easily write the asciidoc documentation
    private Map<String, Object> parseBase64Document(String base64, AttachmentProcessor processor) throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", base64);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> attachmentData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        return attachmentData;
    }

    protected String getAsBase64(String filename) throws Exception {
        String path = "/org/elasticsearch/ingest/attachment/test/sample-files/" + filename;
        try (InputStream is = AttachmentProcessorTests.class.getResourceAsStream(path)) {
            byte bytes[] = IOUtils.toByteArray(is);
            return Base64.getEncoder().encodeToString(bytes);
        }
    }
}
