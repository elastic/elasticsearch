/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AttachmentProcessorTests extends ESTestCase {

    private Processor processor;

    @Before
    public void createStandardProcessor() {
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.allOf(AttachmentProcessor.Property.class),
            10000,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
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
        // some metadata are not present in the html doc
        // "date", "metadata_date", "comments", "modified", "modifier", "print_date", "relation", "creator_tool", "altitude"
        // "identifier", "longitude", "publisher", "description", "latitude", "format", "source", "coverage"
        // "rating", "type", "contributor", "rights"
        // we are only trying with content, title, author, keywords, content_type and content_length.
        ArrayList<AttachmentProcessor.Property> fieldsList = new ArrayList<>(
            EnumSet.of(
                AttachmentProcessor.Property.CONTENT,
                AttachmentProcessor.Property.TITLE,
                AttachmentProcessor.Property.AUTHOR,
                AttachmentProcessor.Property.KEYWORDS,
                AttachmentProcessor.Property.CONTENT_TYPE,
                AttachmentProcessor.Property.CONTENT_LENGTH
            )
        );
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
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            selectedProperties,
            10000,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );

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

        assertThat(
            attachmentData.keySet(),
            containsInAnyOrder(
                "content",
                "language",
                "date",
                "author",
                "content_type",
                "content_length",
                "modifier",
                "modified",
                "publisher"
            )
        );
        assertThat(attachmentData.get("content"), is(notNullValue()));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("date"), is("2012-10-12T11:17:00Z"));
        assertThat(attachmentData.get("author"), is("Windows User"));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
        assertThat(
            attachmentData.get("content_type").toString(),
            is("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        );
        assertThat(attachmentData.get("modifier").toString(), is("Luka Lampret"));
        assertThat(attachmentData.get("modified").toString(), is("2015-02-20T11:36:00Z"));
        assertThat(attachmentData.get("publisher").toString(), is("JDI"));
    }

    public void testWordDocumentWithVisioSchema() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.docx", processor);

        assertThat(
            attachmentData.keySet(),
            containsInAnyOrder(
                "content",
                "language",
                "date",
                "author",
                "content_type",
                "content_length",
                "modifier",
                "modified",
                "print_date"
            )
        );
        assertThat(attachmentData.get("content").toString(), containsString("Table of Contents"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("date"), is("2015-01-06T18:07:00Z"));
        assertThat(attachmentData.get("author"), is(notNullValue()));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
        assertThat(
            attachmentData.get("content_type").toString(),
            is("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        );
        assertThat(attachmentData.get("modifier").toString(), is("Chris Dufour"));
        assertThat(attachmentData.get("modified").toString(), is("2016-12-04T16:58:00Z"));
        assertThat(attachmentData.get("print_date").toString(), is("2015-01-05T19:12:00Z"));
    }

    public void testLegacyWordDocumentWithVisioSchema() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.doc", processor);

        assertThat(
            attachmentData.keySet(),
            containsInAnyOrder(
                "content",
                "language",
                "date",
                "author",
                "content_type",
                "content_length",
                "modifier",
                "modified",
                "print_date"
            )
        );
        assertThat(attachmentData.get("content").toString(), containsString("Table of Contents"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("date"), is("2016-12-16T15:04:00Z"));
        assertThat(attachmentData.get("author"), is(notNullValue()));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
        assertThat(attachmentData.get("content_type").toString(), is("application/msword"));
        assertThat(attachmentData.get("modifier").toString(), is("David Pilato"));
        assertThat(attachmentData.get("modified").toString(), is("2016-12-16T15:04:00Z"));
        assertThat(attachmentData.get("print_date").toString(), is("2015-01-05T19:12:00Z"));
    }

    public void testPdf() throws Exception {
        Map<String, Object> attachmentData = parseDocument("test.pdf", processor);
        assertThat(
            attachmentData.get("content"),
            is("This is a test, with umlauts, from München\n\nAlso contains newlines for testing.\n\nAnd one more.")
        );
        assertThat(attachmentData.get("content_type").toString(), is("application/pdf"));
        assertThat(attachmentData.get("content_length"), is(notNullValue()));
    }

    public void testVisioIsExcluded() throws Exception {
        Map<String, Object> attachmentData = parseDocument("issue-22077.vsdx", processor);
        assertThat(attachmentData.get("content"), nullValue());
        assertThat(attachmentData.get("content_type"), is("application/vnd.ms-visio.drawing"));
        assertThat(attachmentData.get("content_length"), is(0L));
    }

    public void testEncryptedWithPasswordPdf() throws Exception {
        /*
         * This tests that a PDF that has been encrypted with a password fails in the way expected
         */
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parseDocument("encrypted.pdf", processor));
        assertThat(e.getDetailedMessage(), containsString("document is encrypted"));
    }

    public void testEncryptedWithKeyPdf() throws Exception {
        /*
         * This tests that a PDF that has been encrypted with a public key fails in the way expected
         */
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> parseDocument("encrypted-with-key.pdf", processor)
        );
        assertThat(e.getDetailedMessage(), containsString("document is encrypted"));
    }

    public void testHtmlDocument() throws Exception {
        Map<String, Object> attachmentData = parseDocument("htmlWithEmptyDateMeta.html", processor);

        assertThat(
            attachmentData.keySet(),
            containsInAnyOrder("language", "content", "author", "keywords", "title", "content_type", "content_length")
        );
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

        assertThat(
            attachmentData.keySet(),
            containsInAnyOrder(
                "language",
                "content",
                "author",
                "title",
                "content_type",
                "content_length",
                "date",
                "keywords",
                "identifier",
                "contributor",
                "publisher",
                "description"
            )
        );
        assertThat(attachmentData.get("content_type").toString(), containsString("application/epub+zip"));
        assertThat(attachmentData.get("identifier").toString(), is("1234567890"));
        assertThat(attachmentData.get("contributor").toString(), is("no-one"));
        assertThat(attachmentData.get("publisher").toString(), is("Apache"));
        assertThat(attachmentData.get("description").toString(), is("This is an ePub test publication for Tika."));
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
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap("source_field", null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "randomTarget",
            null,
            10,
            true,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "randomTarget",
            null,
            10,
            true,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap("source_field", null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "randomTarget",
            null,
            10,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot parse."));
    }

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "randomTarget",
            null,
            10,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    private Map<String, Object> parseDocument(String file, Processor attachmentProcessor) throws Exception {
        return parseDocument(file, attachmentProcessor, new HashMap<>());
    }

    private Map<String, Object> parseDocument(String file, Processor attachmentProcessor, Map<String, Object> optionalFields)
        throws Exception {
        return parseDocument(file, attachmentProcessor, optionalFields, false);
    }

    private Map<String, Object> parseDocument(
        String file,
        Processor attachmentProcessor,
        Map<String, Object> optionalFields,
        boolean includeResourceName
    ) throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", getAsBinaryOrBase64(file));
        if (includeResourceName) {
            document.put("resource_name", file);
        }
        document.putAll(optionalFields);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        attachmentProcessor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> attachmentData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        return attachmentData;
    }

    public void testIndexedChars() throws Exception {
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.allOf(AttachmentProcessor.Property.class),
            19,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );

        Map<String, Object> attachmentData = parseDocument("text-in-english.txt", processor);

        assertThat(attachmentData.keySet(), containsInAnyOrder("language", "content", "content_type", "content_length"));
        assertThat(attachmentData.get("language"), is("en"));
        assertThat(attachmentData.get("content"), is("\"God Save the Queen"));
        assertThat(attachmentData.get("content_type").toString(), containsString("text/plain"));
        assertThat(attachmentData.get("content_length"), is(19L));

        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.allOf(AttachmentProcessor.Property.class),
            19,
            false,
            "max_length",
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );

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
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.allOf(AttachmentProcessor.Property.class),
            100,
            false,
            null,
            "resource_name",
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );

        Map<String, Object> attachmentData = parseDocument(
            "text-cjk-big5.txt",
            processor,
            Collections.singletonMap("max_length", 100),
            true
        );

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

    public void testRemoveBinary() throws Exception {
        {
            // Test the default behavior.
            Map<String, Object> document = new HashMap<>();
            document.put("source_field", getAsBinaryOrBase64("text-in-english.txt"));
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.hasField("source_field"), is(true));
        }
        {
            // Remove the binary field.
            processor = new AttachmentProcessor(
                randomAlphaOfLength(10),
                null,
                "source_field",
                "target_field",
                EnumSet.allOf(AttachmentProcessor.Property.class),
                10000,
                false,
                null,
                null,
                true,
                -1,
                new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
                "",
                new SetOnce<>()
            );
            Map<String, Object> document = new HashMap<>();
            document.put("source_field", getAsBinaryOrBase64("text-in-english.txt"));
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
            processor.execute(ingestDocument);
            assertThat(ingestDocument.hasField("source_field"), is(false));
        }
    }

    public void testMaxFieldBytesViolated() {
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            10000,
            false,
            null,
            null,
            true,
            randomIntBetween(1, 4),
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
        ElasticsearchParseException ex = expectThrows(
            ElasticsearchParseException.class,
            () -> parseDocument("text-in-english.txt", processor)
        );
        assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
        assertThat(ex.getMessage(), containsString("source_field"));
    }

    public void testMaxAttachmentFieldSizeEnforcedOnBase64BeforeDecode() {
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            10000,
            false,
            null,
            null,
            true,
            100,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
        // Each "QQ==" is four bytes and decodes to two bytes; raw field size exceeds the cap while decoded size stays modest.
        String largeBase64 = "QQ==".repeat(50);
        assertThat(largeBase64.length(), greaterThan(100));
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", largeBase64);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ElasticsearchParseException ex = expectThrows(ElasticsearchParseException.class, () -> processor.execute(ingestDocument));
        assertThat(ex.getMessage(), containsString("attachment field size"));
        assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
        assertThat(ex.getMessage(), containsString("source_field"));
    }

    public void testMaxFieldBytesNotViolated() throws Exception {
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            10000,
            false,
            null,
            null,
            true,
            randomIntBetween(100000, 200000),
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            new SetOnce<>()
        );
        final var attachmentData = parseDocument("text-in-english.txt", processor);
        assertNotNull(attachmentData);
    }

    public void testMaxFieldBytesVariousConfigurations() throws Exception {
        final int bytes = randomIntBetween(-1, 10);
        var nodeLimit = new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE);
        if (randomBoolean() && bytes >= 0) {
            // Put a non-stricter node limit that should not matter
            nodeLimit = new RelativeByteSizeValue(ByteSizeValue.ofBytes(randomLongBetween(bytes, bytes * 2)));
        }
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            100000000,
            false,
            null,
            null,
            true,
            bytes,
            nodeLimit,
            "",
            new SetOnce<>()
        );

        if (bytes < 0) {
            // No exception should be thrown
            assertThat(parseRandomStringAttachmentAndGetTargetField(randomIntBetween(1, 100), processor), notNullValue());
        } else if (bytes == 0) {
            // Exception should always be thrown
            ElasticsearchParseException ex = expectThrows(
                ElasticsearchParseException.class,
                () -> parseRandomStringAttachmentAndGetTargetField(randomIntBetween(1, 1000), processor)
            );
            assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
        } else {
            // Exception should not be thrown for smaller documents
            assertThat(parseRandomStringAttachmentAndGetTargetField(randomIntBetween(0, bytes - 1), processor), notNullValue());
            // Exception should be thrown for larger documents
            ElasticsearchParseException ex = expectThrows(
                ElasticsearchParseException.class,
                () -> parseRandomStringAttachmentAndGetTargetField(bytes + randomIntBetween(1, 10), processor)
            );
            assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
        }
    }

    public void testNodeMaxFieldSizeVariousConfigurations() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("target_field", "target_field");
        config.put("remove_binary", true);
        if (randomBoolean()) {
            // Absolute bytes
            final int bytes = randomIntBetween(-1, 10);
            final var expectedRelativeByteSizeValue = new RelativeByteSizeValue(ByteSizeValue.ofBytes(bytes));
            final var testFactory = new AttachmentProcessor.Factory(
                Settings.builder().put("ingest.attachment.max_field_size", bytes + "b").build(),
                new SetOnce<>()
            );
            int expectedProcessorBytes = -1;
            if (randomBoolean() && bytes >= 0) {
                // Put a non-stricter processor limit that should not matter
                expectedProcessorBytes = randomIntBetween(bytes, 100);
                config.put("max_field_bytes", expectedProcessorBytes);
            }
            AttachmentProcessor processor = testFactory.create(null, "t", null, config, null);
            assertThat(processor.getMaxFieldBytesFromProcessor(), equalTo(expectedProcessorBytes));
            assertThat(processor.getMaxFieldSizeFromNode().getAbsolute(), equalTo(expectedRelativeByteSizeValue.getAbsolute()));
            if (bytes < 0) {
                // No exception should be thrown
                assertThat(parseRandomStringAttachmentAndGetTargetField(randomIntBetween(1, 100), processor), notNullValue());
            } else if (bytes == 0) {
                // Exception should always be thrown
                ElasticsearchParseException ex = expectThrows(
                    ElasticsearchParseException.class,
                    () -> parseRandomStringAttachmentAndGetTargetField(randomIntBetween(1, 1000), processor)
                );
                assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
            } else {
                // Exception should not be thrown for smaller documents
                assertThat(parseRandomStringAttachmentAndGetTargetField(randomIntBetween(0, bytes - 1), processor), notNullValue());
                // Exception should be thrown for larger documents
                ElasticsearchParseException ex = expectThrows(
                    ElasticsearchParseException.class,
                    () -> parseRandomStringAttachmentAndGetTargetField(bytes + randomIntBetween(1, 10), processor)
                );
                assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
            }
        } else {
            // Ratio
            long percent = randomLongBetween(0L, 2L);
            final var expectedRelativeByteSizeValue = new RelativeByteSizeValue(new RatioValue(percent * 1.0));
            final var testFactory = new AttachmentProcessor.Factory(
                Settings.builder().put("ingest.attachment.max_field_size", percent + "%").build(),
                new SetOnce<>()
            );
            long heapMaxBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
            assertThat(heapMaxBytes, greaterThan(0L));
            long maxAttachmentBytes = heapMaxBytes * percent / 100L;
            int maxAttachmentBytesInt = Math.toIntExact(Math.min(maxAttachmentBytes, Integer.MAX_VALUE));
            int expectedProcessorBytes = -1;
            if (randomBoolean() && maxAttachmentBytesInt >= 0) {
                // Put a non-stricter processor limit that should not matter
                expectedProcessorBytes = randomIntBetween(maxAttachmentBytesInt, Math.min(Integer.MAX_VALUE, maxAttachmentBytesInt * 2));
                config.put("max_field_bytes", expectedProcessorBytes);
            }
            AttachmentProcessor processor = testFactory.create(null, "t", null, config, null);
            assertThat(processor.getMaxFieldBytesFromProcessor(), equalTo(expectedProcessorBytes));
            assertThat(
                processor.getMaxFieldSizeFromNode().getRatio().getAsPercent(),
                equalTo(expectedRelativeByteSizeValue.getRatio().getAsPercent())
            );
            if (percent == 0L) {
                // Exception should always be thrown
                ElasticsearchParseException ex = expectThrows(
                    ElasticsearchParseException.class,
                    () -> parseRandomStringAttachmentAndGetTargetField(randomIntBetween(1, 1000), processor)
                );
                assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
            } else {
                // Exception should not be thrown for smaller documents
                assertThat(
                    parseRandomStringAttachmentAndGetTargetField(randomIntBetween(0, maxAttachmentBytesInt / 2), processor),
                    notNullValue()
                );
                if (maxAttachmentBytes < Integer.MAX_VALUE) {
                    // Exception should be thrown for larger documents
                    int attachmentBytes = Math.toIntExact(Math.min(Integer.MAX_VALUE, maxAttachmentBytes + randomLongBetween(100, 1000)));
                    ElasticsearchParseException ex = expectThrows(
                        ElasticsearchParseException.class,
                        () -> parseRandomStringAttachmentAndGetTargetField(attachmentBytes, processor)
                    );
                    assertThat(ex.getMessage(), containsString("exceeding the maximum allowed"));
                }
            }
        }
    }

    public void testNodeMaxFieldSizeViolationUsesCustomMessageWhenConfigured() throws Exception {
        String customMessage = "custom";
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("target_field", "target_field");
        config.put("remove_binary", true);
        AttachmentProcessor processor = new AttachmentProcessor.Factory(
            Settings.builder()
                .put(AttachmentProcessor.MAX_FIELD_SIZE_SETTING.getKey(), "1b")
                .put(AttachmentProcessor.MAX_FIELD_SIZE_MESSAGE_SUFFIX_SETTING.getKey(), customMessage)
                .build(),
            new SetOnce<>()
        ).create(null, "t", null, config, null);

        ElasticsearchParseException ex = expectThrows(
            ElasticsearchParseException.class,
            () -> parseRandomStringAttachmentAndGetTargetField(2, processor)
        );
        assertThat(
            ex.getMessage(),
            equalTo("field [source_field] has an attachment field size of [2] bytes exceeding the maximum allowed input size custom")
        );
    }

    public void testSizeMetricsSuccess() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        SetOnce<AttachmentIngestMetrics> metricsRef = new SetOnce<>();
        metricsRef.set(new AttachmentIngestMetrics(meterRegistry));
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            10000,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            metricsRef
        );
        int bytes = randomIntBetween(1, 100);
        assertThat(parseRandomStringAttachmentAndGetTargetField(bytes, processor), notNullValue());
        assertHistogramLastValue(meterRegistry, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED, bytes);
        assertHistogramLastValue(meterRegistry, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_PROCESSED, bytes);
    }

    public void testSizeMetricsOneMebibyteDocument() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        SetOnce<AttachmentIngestMetrics> metricsRef = new SetOnce<>();
        metricsRef.set(new AttachmentIngestMetrics(meterRegistry));
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            10000,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            metricsRef
        );
        int bytes = 1_048_576;
        assertThat(parseRandomStringAttachmentAndGetTargetField(bytes, processor), notNullValue());
        assertHistogramLastValue(meterRegistry, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED, bytes);
        assertHistogramLastValue(meterRegistry, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_PROCESSED, bytes);
    }

    public void testSizeMetricsIgnoreMissing() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        SetOnce<AttachmentIngestMetrics> metricsRef = new SetOnce<>();
        metricsRef.set(new AttachmentIngestMetrics(meterRegistry));
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "randomTarget",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            10,
            true,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            metricsRef
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        processor.execute(ingestDocument);
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED),
            hasSize(0)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_PROCESSED),
            hasSize(0)
        );
    }

    public void testSizeMetricsForOversize() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        SetOnce<AttachmentIngestMetrics> metricsRef = new SetOnce<>();
        metricsRef.set(new AttachmentIngestMetrics(meterRegistry));
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.of(AttachmentProcessor.Property.CONTENT),
            10000,
            false,
            null,
            null,
            false,
            0,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            metricsRef
        );

        int bytes = randomIntBetween(1, 100);
        expectThrows(ElasticsearchParseException.class, () -> parseRandomStringAttachmentAndGetTargetField(bytes, processor));

        assertHistogramLastValue(meterRegistry, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED, bytes);
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_PROCESSED),
            hasSize(0)
        );
    }

    public void testSizeMetricsWhenProcessingFails() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        SetOnce<AttachmentIngestMetrics> metricsRef = new SetOnce<>();
        metricsRef.set(new AttachmentIngestMetrics(meterRegistry));
        processor = new AttachmentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            EnumSet.allOf(AttachmentProcessor.Property.class),
            10000,
            false,
            null,
            null,
            false,
            -1,
            new RelativeByteSizeValue(ByteSizeValue.MINUS_ONE),
            "",
            metricsRef
        );
        expectThrows(ElasticsearchParseException.class, () -> parseDocument("encrypted.pdf", processor));
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED),
            hasSize(greaterThanOrEqualTo(1))
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED)
                .getLast()
                .getDouble(),
            greaterThan(0.0)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, AttachmentIngestMetrics.RAW_FIELD_SIZE_IN_MEBIBYTES_PROCESSED),
            hasSize(0)
        );
    }

    private static void assertHistogramLastValue(RecordingMeterRegistry meterRegistry, String metricName, long rawBytes) {
        var measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, metricName);
        assertThat(measurements, hasSize(greaterThanOrEqualTo(1)));
        assertThat(measurements.get(measurements.size() - 1).getDouble(), closeTo(AttachmentIngestMetrics.toMebibytes(rawBytes), 1e-12));
    }

    private static Object parseRandomStringAttachmentAndGetTargetField(int bytes, Processor processor) throws Exception {
        assertThat(bytes, greaterThanOrEqualTo(0));
        String str = randomAlphanumericOfLength(bytes);
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", str.getBytes(StandardCharsets.US_ASCII));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        return ingestDocument.getSourceAndMetadata().get("target_field");
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
