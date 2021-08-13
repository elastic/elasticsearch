/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AttachmentProcessorTests extends ESTestCase {

    private AttachmentProcessor processor;

    @Before
    public void createStandardProcessor() {
        processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "target_field", EnumSet.allOf(AttachmentProcessor.Property.class), 10000, false, null, null);
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
        processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "target_field", selectedProperties, 10000, false, null, null);

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

    public void testWordDocumentWithVisioSchema() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.docx", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("content", "language", "date", "author", "content_type",
            "content_length"));
        assertThat(attachmentData.get("content").toString(), containsString("Table of Contents"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("date"), is("2015-01-06T18:07:00Z"));
        assertThat(attachmentData.get("author"), is(notNullValue()));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
        assertThat(attachmentData.get("content_type").toString(),
            is("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
    }

    public void testLegacyWordDocumentWithVisioSchema() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.doc", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("content", "language", "date", "author", "content_type",
            "content_length"));
        assertThat(attachmentData.get("content").toString(), containsString("Table of Contents"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("date"), is("2016-12-16T15:04:00Z"));
        assertThat(attachmentData.get("author"), is(notNullValue()));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
        assertThat(attachmentData.get("content_type").toString(),
            is("application/msword"));
    }

    public void testPdf() throws Exception {
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);
        assertThat(attachmentData.get("content"),
            is("This is a test, with umlauts, from München\n\nAlso contains newlines for testing.\n\nAnd one more."));
        assertThat(attachmentData.get("content_type").toString(), is("application/pdf"));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
    }

    public void testVisioIsExcluded() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.vsdx", processor);
        assertThat(attachmentData.get("content"), nullValue());
        assertThat(attachmentData.get("content_type"), is("application/vnd.ms-visio.drawing"));
        assertThat(attachmentData.get("content_length"), is(0L));
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

    // See (https://issues.apache.org/jira/browse/COMPRESS-432) for information
    // about the issue that causes a zip file to hang in Tika versions prior to 1.18.
    public void testZipFileDoesNotHang() throws Exception {
        parseDocument("bad_tika.zip", processor);
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
        Processor processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "randomTarget", null, 10, true, null, null);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "randomTarget", null, 10, true, null, null);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "randomTarget", null, 10, false, null, null);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot parse."));
    }

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "randomTarget", null, 10, false, null, null);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    private Map<String, Object> parseDocument(String file, AttachmentProcessor processor) throws Exception {
        return parseDocument(file, processor, new HashMap<>());
    }

    private Map<String, Object> parseDocument(String file, AttachmentProcessor processor, Map<String, Object> optionalFields)
        throws Exception {
        return parseDocument(file, processor, optionalFields, false);
    }

    private Map<String, Object> parseDocument(String file, AttachmentProcessor processor, Map<String, Object> optionalFields,
                                              boolean includeResourceName)
        throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", getAsBinaryOrBase64(file));
        if (includeResourceName) {
            document.put("resource_name", file);
        }
        document.putAll(optionalFields);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> attachmentData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        return attachmentData;
    }

    public void testIndexedChars() throws Exception {
        processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "target_field", EnumSet.allOf(AttachmentProcessor.Property.class), 19, false, null, null);

        Map<String, Object> attachmentData = parseDocument("text-in-english.txt", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("content"), is("\"God Save the Queen"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_length"), is(19L));

        processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "target_field", EnumSet.allOf(AttachmentProcessor.Property.class), 19, false, "max_length", null);

        attachmentData = parseDocument("text-in-english.txt", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("content"), is("\"God Save the Queen"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_length"), is(19L));

        attachmentData = parseDocument("text-in-english.txt", processor, Collections.singletonMap("max_length", 10));

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("language"), is("sk"));
        assertThat(attachmentData.get("content"), is("\"God Save"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_length"), is(10L));

        attachmentData = parseDocument("text-in-english.txt", processor, Collections.singletonMap("max_length", 100));

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("content"), is("\"God Save the Queen\" (alternatively \"God Save the King\""));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_length"), is(56L));

    }

    public void testIndexedCharsWithResourceName() throws Exception {
        processor = new AttachmentProcessor(randomAlphaOfLength(10), null, "source_field",
            "target_field", EnumSet.allOf(AttachmentProcessor.Property.class), 100,
            false, null, "resource_name");

        Map<String, Object> attachmentData = parseDocument("text-cjk-big5.txt", processor, Collections.singletonMap("max_length", 100),
            true);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("content").toString(), containsString("碩鼠碩鼠，無食我黍！"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_type").toString(), containsString("charset=Big5"));
        assertThat(attachmentData.get("content_length"), is(100L));

        attachmentData = parseDocument("text-cjk-gbk.txt", processor, Collections.singletonMap("max_length", 100), true);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("content").toString(), containsString("硕鼠硕鼠，无食我黍！"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_type").toString(), containsString("charset=GB18030"));
        assertThat(attachmentData.get("content_length"), is(100L));

        attachmentData = parseDocument("text-cjk-euc-jp.txt", processor, Collections.singletonMap("max_length", 100), true);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("content").toString(), containsString("碩鼠よ碩鼠よ、" + System.lineSeparator() + "我が黍を食らう無かれ！"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_type").toString(), containsString("charset=EUC-JP"));
        assertThat(attachmentData.get("content_length"), is(100L));
    }

    private Object getAsBinaryOrBase64(String filename) throws Exception {
        String path = "/org/elasticsearch/ingest/attachment/test/sample-files/" + filename;
        try (InputStream is = AttachmentProcessorTests.class.getResourceAsStream(path)) {
            byte bytes[] = IOUtils.toByteArray(is);
            // behave like CBOR from time to time
            if (rarely()) {
                return bytes;
            } else {
                return Base64.getEncoder().encodeToString(bytes);
            }
        }
    }
}
