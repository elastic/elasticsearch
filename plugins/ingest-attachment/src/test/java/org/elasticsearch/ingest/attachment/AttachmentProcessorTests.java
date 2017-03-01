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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.AUTHOR;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.CONTENT;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.CONTENT_LENGTH;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.CONTENT_TYPE;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.DATE;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.KEYWORDS;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.LANGUAGE;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.RESERVED_PROPERTIES;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.RESERVED_PROPERTIES_KEYS;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.TITLE;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.buildCharacterRunAutomaton;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

public class AttachmentProcessorTests extends ESTestCase {

    private AttachmentProcessor processor;

    @Before
    public void createStandardProcessor() throws IOException {
        // We test the default behavior which is extracting all metadata but the raw_metadata
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            RESERVED_PROPERTIES_KEYS, 10000, false, null);
    }

    public void testEnglishTextDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-in-english.txt", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(LANGUAGE, CONTENT, CONTENT_TYPE, CONTENT_LENGTH));
        assertThat(attachmentData.get(LANGUAGE), is("en"));
        assertThat(attachmentData.get(CONTENT), is("\"God Save the Queen\" (alternatively \"God Save the King\""));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(), containsString("text/plain"));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(notNullValue()));
    }

    public void testHtmlDocumentWithRandomFields() throws Exception {
        // date is not present in the html doc
        Set<String> allButDate = Sets.difference(RESERVED_PROPERTIES, Sets.newHashSet(DATE));
        Set<String> selectedProperties = randomReservedProperties(allButDate);

        int expectedFields = selectedProperties.size();

        if (randomBoolean()) {
            selectedProperties.add(DATE);
        }

        // Convert to reserved property keys
        Set<String> reservedPropertyKeys = selectedProperties.stream()
            .map(AttachmentProcessor::asReservedProperty)
            .collect(Collectors.toSet());

        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            reservedPropertyKeys, 10000, false, null);

        Map<String, Object> attachmentData = parseDocument("htmlWithEmptyDateMeta.html", processor);
        assertThat(attachmentData.keySet(), hasSize(expectedFields));

        // Check that we have all fields but date
        selectedProperties.stream()
            .filter(reservedProperty -> reservedProperty.equals(DATE) == false)
            .forEach(fieldName -> assertThat(attachmentData, hasKey(fieldName)));
    }

    public void testFrenchTextDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-in-french.txt", processor);

        assertThat(attachmentData.keySet(), hasItem(LANGUAGE));
        assertThat(attachmentData.get(LANGUAGE), is("fr"));
    }

    public void testUnknownLanguageDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-gibberish.txt", processor);

        assertThat(attachmentData.keySet(), hasItem(LANGUAGE));
        // lt seems some standard for not detected
        assertThat(attachmentData.get(LANGUAGE), is("lt"));
    }

    public void testEmptyTextDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("text-empty.txt", processor);
        assertThat(attachmentData.keySet(), not(hasItem(LANGUAGE)));
    }

    public void testWordDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-104.docx", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(CONTENT, LANGUAGE, DATE, AUTHOR, CONTENT_TYPE, CONTENT_LENGTH));
        assertThat(attachmentData.get(CONTENT), is(notNullValue()));
        assertThat(attachmentData.get(LANGUAGE), is("en"));
        assertThat(attachmentData.get(DATE), is("2012-10-12T11:17:00Z"));
        assertThat(attachmentData.get(AUTHOR), is("Windows User"));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(notNullValue()));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(),
            is("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
    }

    public void testWordDocumentWithVisioSchema() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.docx", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(CONTENT, LANGUAGE, DATE, AUTHOR, CONTENT_TYPE, CONTENT_LENGTH));
        assertThat(attachmentData.get(CONTENT).toString(), containsString("Table of Contents"));
        assertThat(attachmentData.get(LANGUAGE), is("en"));
        assertThat(attachmentData.get(DATE), is("2015-01-06T18:07:00Z"));
        assertThat(attachmentData.get(AUTHOR), is(notNullValue()));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(notNullValue()));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(),
            is("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
    }

    public void testLegacyWordDocumentWithVisioSchema() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.doc", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(CONTENT, LANGUAGE, DATE, AUTHOR, CONTENT_TYPE, CONTENT_LENGTH));
        assertThat(attachmentData.get(CONTENT).toString(), containsString("Table of Contents"));
        assertThat(attachmentData.get(LANGUAGE), is("en"));
        assertThat(attachmentData.get(DATE), is("2016-12-16T15:04:00Z"));
        assertThat(attachmentData.get(AUTHOR), is(notNullValue()));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(notNullValue()));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(),
            is("application/msword"));
    }

    public void testPdf() throws Exception {
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);
        assertThat(attachmentData.get(CONTENT),
                is("This is a test, with umlauts, from München\n\nAlso contains newlines for testing.\n\nAnd one more."));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(), is("application/pdf"));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(notNullValue()));
    }

    public void testVisioIsExcluded() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.vsdx", processor);
        assertThat(attachmentData.get(CONTENT), nullValue());
        assertThat(attachmentData.get(CONTENT_TYPE), is("application/vnd.ms-visio.drawing"));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(0L));
    }

    public void testEncryptedPdf() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parseDocument("encrypted.pdf", processor));
        assertThat(e.getDetailedMessage(), containsString("document is encrypted"));
    }

    public void testHtmlDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("htmlWithEmptyDateMeta.html", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(LANGUAGE, CONTENT, AUTHOR, TITLE, CONTENT_TYPE, CONTENT_LENGTH, KEYWORDS));
        assertThat(attachmentData.get(LANGUAGE), is("en"));
        assertThat(attachmentData.get(CONTENT), is(notNullValue()));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(notNullValue()));
        assertThat(attachmentData.get(AUTHOR), is("kimchy"));
        assertThat(attachmentData.get(KEYWORDS), is("elasticsearch,cool,bonsai"));
        assertThat(attachmentData.get(TITLE), is("Hello"));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(), containsString("text/html"));
    }

    public void testXHtmlDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("testXHTML.html", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(LANGUAGE, CONTENT, AUTHOR, TITLE, CONTENT_TYPE, CONTENT_LENGTH));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(), containsString("application/xhtml+xml"));
    }

    public void testEpubDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("testEPUB.epub", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(LANGUAGE, CONTENT, AUTHOR, TITLE, CONTENT_TYPE, CONTENT_LENGTH, DATE,
            KEYWORDS));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(), containsString("application/epub+zip"));
    }

    // no real detection, just rudimentary
    public void testAsciidocDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("asciidoc.asciidoc", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder(LANGUAGE, CONTENT_TYPE, CONTENT, CONTENT_LENGTH));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(), containsString("text/plain"));
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

        assertThat(attachmentData.keySet(), containsInAnyOrder(LANGUAGE, CONTENT, CONTENT_TYPE, CONTENT_LENGTH));
        assertThat(attachmentData.get(LANGUAGE), is("en"));
        assertThat(attachmentData.get(CONTENT), is("\"God Save the Queen\" (alternatively \"God Save the King\""));
        assertThat(attachmentData.get(CONTENT_TYPE).toString(), containsString("text/plain"));
        assertThat(attachmentData.get(CONTENT_LENGTH), is(notNullValue()));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget",
            Collections.emptySet(), 10, true, null);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget",
            Collections.emptySet(), 10, true, null);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget",
            Collections.emptySet(), 10, false, null);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot parse."));
    }

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "randomTarget",
            Collections.emptySet(), 10, false, null);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    public void testRawMetadataFromWordDocument() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            Collections.emptySet(), 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("*")));

        Map<String, Object> attachmentData = parseDocument("issue-104.docx", processor);

        // An easy way to generate all metadata assertions is by running
        /*for (Map.Entry<String, Object> entry : attachmentData.entrySet()) {
            logger.info("assertThat(attachmentData, hasEntry(\"{}\", \"{}\"));", entry.getKey(), entry.getValue());
        }*/

        assertThat(attachmentData, hasEntry("date", "2015-02-20T11:36:00Z"));
        assertThat(attachmentData, hasEntry("cp:revision", "22"));
        assertThat(attachmentData, hasEntry("Total-Time", "6"));
        assertThat(attachmentData, hasEntry("extended-properties:AppVersion", "15.0000"));
        assertThat(attachmentData, hasEntry("meta:paragraph-count", "1"));
        assertThat(attachmentData, hasEntry("meta:word-count", "15"));
        assertThat(attachmentData, hasEntry("dc:creator", "Windows User"));
        assertThat(attachmentData, hasEntry("extended-properties:Company", "JDI"));
        assertThat(attachmentData, hasEntry("Word-Count", "15"));
        assertThat(attachmentData, hasEntry("dcterms:created", "2012-10-12T11:17:00Z"));
        assertThat(attachmentData, hasEntry("meta:line-count", "1"));
        assertThat(attachmentData, hasEntry("Last-Modified", "2015-02-20T11:36:00Z"));
        assertThat(attachmentData, hasEntry("dcterms:modified", "2015-02-20T11:36:00Z"));
        assertThat(attachmentData, hasEntry("Last-Save-Date", "2015-02-20T11:36:00Z"));
        assertThat(attachmentData, hasEntry("meta:character-count", "92"));
        assertThat(attachmentData, hasEntry("Template", "Normal.dotm"));
        assertThat(attachmentData, hasEntry("Line-Count", "1"));
        assertThat(attachmentData, hasEntry("Paragraph-Count", "1"));
        assertThat(attachmentData, hasEntry("meta:save-date", "2015-02-20T11:36:00Z"));
        assertThat(attachmentData, hasEntry("meta:character-count-with-spaces", "106"));
        assertThat(attachmentData, hasEntry("Application-Name", "Microsoft Office Word"));
        assertThat(attachmentData, hasEntry("extended-properties:TotalTime", "6"));
        assertThat(attachmentData, hasEntry("modified", "2015-02-20T11:36:00Z"));
        assertThat(attachmentData, hasEntry("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
        assertThat(attachmentData, hasEntry("X-Parsed-By", "org.apache.tika.parser.microsoft.ooxml.OOXMLParser"));
        assertThat(attachmentData, hasEntry("creator", "Windows User"));
        assertThat(attachmentData, hasEntry("meta:author", "Windows User"));
        assertThat(attachmentData, hasEntry("meta:creation-date", "2012-10-12T11:17:00Z"));
        assertThat(attachmentData, hasEntry("extended-properties:Application", "Microsoft Office Word"));
        assertThat(attachmentData, hasEntry("meta:last-author", "Luka Lampret"));
        assertThat(attachmentData, hasEntry("Creation-Date", "2012-10-12T11:17:00Z"));
        assertThat(attachmentData, hasEntry("xmpTPg:NPages", "1"));
        assertThat(attachmentData, hasEntry("Character-Count-With-Spaces", "106"));
        assertThat(attachmentData, hasEntry("Last-Author", "Luka Lampret"));
        assertThat(attachmentData, hasEntry("Character Count", "92"));
        assertThat(attachmentData, hasEntry("Page-Count", "1"));
        assertThat(attachmentData, hasEntry("Revision-Number", "22"));
        assertThat(attachmentData, hasEntry("Application-Version", "15.0000"));
        assertThat(attachmentData, hasEntry("extended-properties:Template", "Normal.dotm"));
        assertThat(attachmentData, hasEntry("Author", "Windows User"));
        assertThat(attachmentData, hasEntry("publisher", "JDI"));
        assertThat(attachmentData, hasEntry("meta:page-count", "1"));
        assertThat(attachmentData, hasEntry("dc:publisher", "JDI"));
    }

    public void testRawMetadataFromPdf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            Collections.emptySet(), 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("*")));
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);

        // "created" is different depending on the JVM Locale. We skip testing its content
        assertThat(attachmentData, hasEntry("pdf:PDFVersion", "1.4"));
        assertThat(attachmentData, hasEntry("X-Parsed-By", "org.apache.tika.parser.pdf.PDFParser"));
        assertThat(attachmentData, hasEntry("xmp:CreatorTool", "Writer"));
        assertThat(attachmentData, hasEntry("access_permission:modify_annotations", "true"));
        assertThat(attachmentData, hasEntry("access_permission:can_print_degraded", "true"));
        assertThat(attachmentData, hasEntry("meta:creation-date", "2016-09-30T13:19:58Z"));
        assertThat(attachmentData, hasEntry("access_permission:extract_for_accessibility", "true"));
        assertThat(attachmentData, hasEntry("access_permission:assemble_document", "true"));
        assertThat(attachmentData, hasEntry("xmpTPg:NPages", "1"));
        assertThat(attachmentData, hasEntry("Creation-Date", "2016-09-30T13:19:58Z"));
        assertThat(attachmentData, hasEntry("dcterms:created", "2016-09-30T13:19:58Z"));
        assertThat(attachmentData, hasEntry("dc:format", "application/pdf; version=1.4"));
        assertThat(attachmentData, hasEntry("access_permission:extract_content", "true"));
        assertThat(attachmentData, hasEntry("access_permission:can_print", "true"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:creator_tool", "Writer"));
        assertThat(attachmentData, hasEntry("access_permission:fill_in_form", "true"));
        assertThat(attachmentData, hasEntry("pdf:encrypted", "false"));
        assertThat(attachmentData, hasEntry("producer", "LibreOffice 5.2"));
        assertThat(attachmentData, hasEntry("access_permission:can_modify", "true"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:producer", "LibreOffice 5.2"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:created", "2016-09-30T13:19:58Z"));
        assertThat(attachmentData, hasEntry("Content-Type", "application/pdf"));
    }

    public void testWildcardFilteredRawMetadataFromPdf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            Collections.emptySet(), 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("pdf:*")));
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);

        // We check that we have all expected field starting with "pdf:"
        assertThat(attachmentData, hasEntry("pdf:PDFVersion", "1.4"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:creator_tool", "Writer"));
        assertThat(attachmentData, hasEntry("pdf:encrypted", "false"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:producer", "LibreOffice 5.2"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:created", "2016-09-30T13:19:58Z"));

        // We check that we did not extract any other field
        assertThat(attachmentData, not(hasKey("X-Parsed-By")));
    }

    public void testFilteredRawMetadataFromPdf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            Collections.emptySet(), 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("pdf:PDFVersion")));
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);

        // We check that we have only the expected field
        assertThat(attachmentData, hasEntry("pdf:PDFVersion", "1.4"));

        // We check that we did not extract any other field
        assertThat(attachmentData, not(hasKey("pdf:encrypted")));
    }

    public void testRawMetadataWith2FiltersFromPdf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            Collections.emptySet(), 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("pdf:PDFVersion", "pdf:encrypted")));
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);

        // We check that we have only expected fields
        assertThat(attachmentData, hasEntry("pdf:PDFVersion", "1.4"));
        assertThat(attachmentData, hasEntry("pdf:encrypted", "false"));

        // We check that we did not extract any other field
        assertThat(attachmentData, not(hasKey("pdf:docinfo:creator_tool")));
    }

    public void testWildcardAndFiltersRawMetadataFromPdf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            Collections.emptySet(), 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("pdf:PDFVersion", "pdf:*")));
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);

        // We check that we have all expected field starting with "pdf:"
        assertThat(attachmentData, hasEntry("pdf:PDFVersion", "1.4"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:creator_tool", "Writer"));
        assertThat(attachmentData, hasEntry("pdf:encrypted", "false"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:producer", "LibreOffice 5.2"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:created", "2016-09-30T13:19:58Z"));

        // We check that we did not extract any other field
        assertThat(attachmentData, not(hasKey("X-Parsed-By")));
    }

    public void testFilteredRawMetadataPlusSomeReservedFieldsFromPdf() throws Exception {
        Set<String> selectedProperties = randomReservedProperties();

        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            selectedProperties, 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("pdf:*")));
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);

        // We check that we have all expected field starting with "pdf:"
        assertThat(attachmentData, hasEntry("pdf:PDFVersion", "1.4"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:creator_tool", "Writer"));
        assertThat(attachmentData, hasEntry("pdf:encrypted", "false"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:producer", "LibreOffice 5.2"));
        assertThat(attachmentData, hasEntry("pdf:docinfo:created", "2016-09-30T13:19:58Z"));

        // We check that we did not extract any other field
        assertThat(attachmentData, not(hasKey("X-Parsed-By")));
    }

    private Set<String> randomReservedProperties() {
        return randomReservedProperties(RESERVED_PROPERTIES);
    }

    private Set<String> randomReservedProperties(Set<String> fieldsList) {
        Set<String> selectedProperties = new HashSet<>();

        int numFields = randomIntBetween(1, fieldsList.size());
        for (int i = 0; i < numFields; i++) {
            String reservedProperty;
            do {
                reservedProperty = randomFrom(fieldsList);
            } while (selectedProperties.add(reservedProperty) == false);
        }

        return selectedProperties;
    }

    public void testRawMetadataFromRtf() throws Exception {
        processor = new AttachmentProcessor(randomAsciiOfLength(10), "source_field", "target_field",
            Collections.emptySet(), 10000, false, buildCharacterRunAutomaton(Sets.newHashSet("*")));
        Map<String, Object> attachmentData =
            parseBase64Document("e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0=", processor);

        assertThat(attachmentData, hasEntry("X-Parsed-By", "org.apache.tika.parser.rtf.RTFParser"));
        assertThat(attachmentData, hasEntry("Content-Type", "application/rtf"));
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
