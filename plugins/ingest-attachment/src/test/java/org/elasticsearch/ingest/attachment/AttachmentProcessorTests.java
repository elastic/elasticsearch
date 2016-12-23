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
        /*
        for (Map.Entry<String, Object> entry : rawMetadata.entrySet()) {
            logger.info("assertThat(rawMetadata.get(\"{}\"), is(\"{}\"));", entry.getKey(), entry.getValue());
        }*/
        assertThat(rawMetadata.get("date"), is("2015-02-20T11:36:00Z"));
        assertThat(rawMetadata.get("cp:revision"), is("22"));
        assertThat(rawMetadata.get("Total-Time"), is("6"));
        assertThat(rawMetadata.get("extended-properties:AppVersion"), is("15.0000"));
        assertThat(rawMetadata.get("meta:paragraph-count"), is("1"));
        assertThat(rawMetadata.get("meta:word-count"), is("15"));
        assertThat(rawMetadata.get("dc:creator"), is("Windows User"));
        assertThat(rawMetadata.get("extended-properties:Company"), is("JDI"));
        assertThat(rawMetadata.get("Word-Count"), is("15"));
        assertThat(rawMetadata.get("dcterms:created"), is("2012-10-12T11:17:00Z"));
        assertThat(rawMetadata.get("meta:line-count"), is("1"));
        assertThat(rawMetadata.get("Last-Modified"), is("2015-02-20T11:36:00Z"));
        assertThat(rawMetadata.get("dcterms:modified"), is("2015-02-20T11:36:00Z"));
        assertThat(rawMetadata.get("Last-Save-Date"), is("2015-02-20T11:36:00Z"));
        assertThat(rawMetadata.get("meta:character-count"), is("92"));
        assertThat(rawMetadata.get("Template"), is("Normal.dotm"));
        assertThat(rawMetadata.get("Line-Count"), is("1"));
        assertThat(rawMetadata.get("Paragraph-Count"), is("1"));
        assertThat(rawMetadata.get("meta:save-date"), is("2015-02-20T11:36:00Z"));
        assertThat(rawMetadata.get("meta:character-count-with-spaces"), is("106"));
        assertThat(rawMetadata.get("Application-Name"), is("Microsoft Office Word"));
        assertThat(rawMetadata.get("extended-properties:TotalTime"), is("6"));
        assertThat(rawMetadata.get("modified"), is("2015-02-20T11:36:00Z"));
        assertThat(rawMetadata.get("Content-Type"), is("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
        assertThat(rawMetadata.get("X-Parsed-By"), is("org.apache.tika.parser.microsoft.ooxml.OOXMLParser"));
        assertThat(rawMetadata.get("creator"), is("Windows User"));
        assertThat(rawMetadata.get("meta:author"), is("Windows User"));
        assertThat(rawMetadata.get("meta:creation-date"), is("2012-10-12T11:17:00Z"));
        assertThat(rawMetadata.get("extended-properties:Application"), is("Microsoft Office Word"));
        assertThat(rawMetadata.get("meta:last-author"), is("Luka Lampret"));
        assertThat(rawMetadata.get("Creation-Date"), is("2012-10-12T11:17:00Z"));
        assertThat(rawMetadata.get("xmpTPg:NPages"), is("1"));
        assertThat(rawMetadata.get("Character-Count-With-Spaces"), is("106"));
        assertThat(rawMetadata.get("Last-Author"), is("Luka Lampret"));
        assertThat(rawMetadata.get("Character Count"), is("92"));
        assertThat(rawMetadata.get("Page-Count"), is("1"));
        assertThat(rawMetadata.get("Revision-Number"), is("22"));
        assertThat(rawMetadata.get("Application-Version"), is("15.0000"));
        assertThat(rawMetadata.get("extended-properties:Template"), is("Normal.dotm"));
        assertThat(rawMetadata.get("Author"), is("Windows User"));
        assertThat(rawMetadata.get("publisher"), is("JDI"));
        assertThat(rawMetadata.get("meta:page-count"), is("1"));
        assertThat(rawMetadata.get("dc:publisher"), is("JDI"));
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

        assertThat(rawMetadata.get("pdf:PDFVersion"), is("1.4"));
        assertThat(rawMetadata.get("X-Parsed-By"), is("org.apache.tika.parser.pdf.PDFParser"));
        assertThat(rawMetadata.get("xmp:CreatorTool"), is("Writer"));
        assertThat(rawMetadata.get("access_permission:modify_annotations"), is("true"));
        assertThat(rawMetadata.get("access_permission:can_print_degraded"), is("true"));
        assertThat(rawMetadata.get("meta:creation-date"), is("2016-09-30T13:19:58Z"));
        // "created" is different depending on the JVM Locale. We just test that it's not null
        assertThat(rawMetadata.get("created"), is(notNullValue()));
        assertThat(rawMetadata.get("access_permission:extract_for_accessibility"), is("true"));
        assertThat(rawMetadata.get("access_permission:assemble_document"), is("true"));
        assertThat(rawMetadata.get("xmpTPg:NPages"), is("1"));
        assertThat(rawMetadata.get("Creation-Date"), is("2016-09-30T13:19:58Z"));
        assertThat(rawMetadata.get("dcterms:created"), is("2016-09-30T13:19:58Z"));
        assertThat(rawMetadata.get("dc:format"), is("application/pdf; version=1.4"));
        assertThat(rawMetadata.get("access_permission:extract_content"), is("true"));
        assertThat(rawMetadata.get("access_permission:can_print"), is("true"));
        assertThat(rawMetadata.get("pdf:docinfo:creator_tool"), is("Writer"));
        assertThat(rawMetadata.get("access_permission:fill_in_form"), is("true"));
        assertThat(rawMetadata.get("pdf:encrypted"), is("false"));
        assertThat(rawMetadata.get("producer"), is("LibreOffice 5.2"));
        assertThat(rawMetadata.get("access_permission:can_modify"), is("true"));
        assertThat(rawMetadata.get("pdf:docinfo:producer"), is("LibreOffice 5.2"));
        assertThat(rawMetadata.get("pdf:docinfo:created"), is("2016-09-30T13:19:58Z"));
        assertThat(rawMetadata.get("Content-Type"), is("application/pdf"));
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

        assertThat(rawMetadata.get("X-Parsed-By"), is("org.apache.tika.parser.rtf.RTFParser"));
        assertThat(rawMetadata.get("Content-Type"), is("application/rtf"));
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
