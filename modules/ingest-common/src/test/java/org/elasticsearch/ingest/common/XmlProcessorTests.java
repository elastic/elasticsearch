/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class XmlProcessorTests extends ESTestCase {

    public void testSimpleXmlDecodeWithTargetField() throws Exception {
        String xml = """
            <catalog>
                <book seq="1">
                    <author>William H. Gaddis</author>
                    <title>The Recognitions</title>
                    <review>One of the great seminal American novels of the 20th century.</review>
                </book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "xml", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> xmlField = (Map<String, Object>) ingestDocument.getFieldValue("xml", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) xmlField.get("catalog");
        @SuppressWarnings("unchecked")
        Map<String, Object> book = (Map<String, Object>) catalog.get("book");

        assertThat(book.get("seq"), equalTo("1"));
        assertThat(book.get("author"), equalTo("William H. Gaddis"));
        assertThat(book.get("title"), equalTo("The Recognitions"));
        assertThat(book.get("review"), equalTo("One of the great seminal American novels of the 20th century."));

        // Original field should remain unchanged
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo(xml));
    }

    public void testXmlDecodeToSameFieldWhenTargetIsField() throws Exception {
        String xml = """
            <?xml version="1.0"?>
            <catalog>
                <book seq="1">
                    <author>William H. Gaddis</author>
                    <title>The Recognitions</title>
                    <review>One of the great seminal American novels of the 20th century.</review>
                </book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) messageField.get("catalog");
        @SuppressWarnings("unchecked")
        Map<String, Object> book = (Map<String, Object>) catalog.get("book");

        assertThat(book.get("seq"), equalTo("1"));
        assertThat(book.get("author"), equalTo("William H. Gaddis"));
        assertThat(book.get("title"), equalTo("The Recognitions"));
        assertThat(book.get("review"), equalTo("One of the great seminal American novels of the 20th century."));
    }

    public void testXmlDecodeWithArray() throws Exception {
        String xml = """
            <?xml version="1.0"?>
            <catalog>
                <book>
                    <author>William H. Gaddis</author>
                    <title>The Recognitions</title>
                    <review>One of the great seminal American novels of the 20th century.</review>
                </book>
                <book>
                    <author>Ralls, Kim</author>
                    <title>Midnight Rain</title>
                    <review>Some review.</review>
                </book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) messageField.get("catalog");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> books = (List<Map<String, Object>>) catalog.get("book");

        assertThat(books.size(), equalTo(2));

        Map<String, Object> firstBook = books.get(0);
        assertThat(firstBook.get("author"), equalTo("William H. Gaddis"));
        assertThat(firstBook.get("title"), equalTo("The Recognitions"));

        Map<String, Object> secondBook = books.get(1);
        assertThat(secondBook.get("author"), equalTo("Ralls, Kim"));
        assertThat(secondBook.get("title"), equalTo("Midnight Rain"));
    }

    public void testXmlDecodeWithToLower() throws Exception {
        String xml = """
            <AuditBase>
                <ContextComponents>
                    <Component>
                        <RelyingParty>N/A</RelyingParty>
                    </Component>
                    <Component>
                        <PrimaryAuth>N/A</PrimaryAuth>
                    </Component>
                </ContextComponents>
            </AuditBase>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, true, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> auditbase = (Map<String, Object>) messageField.get("auditbase");
        @SuppressWarnings("unchecked")
        Map<String, Object> contextcomponents = (Map<String, Object>) auditbase.get("contextcomponents");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> components = (List<Map<String, Object>>) contextcomponents.get("component");

        assertThat(components.size(), equalTo(2));
        assertThat(components.get(0).get("relyingparty"), equalTo("N/A"));
        assertThat(components.get(1).get("primaryauth"), equalTo("N/A"));
    }

    public void testXmlDecodeWithMultipleElements() throws Exception {
        String xml = """
            <?xml version="1.0"?>
            <catalog>
                <book>
                    <author>William H. Gaddis</author>
                    <title>The Recognitions</title>
                    <review>One of the great seminal American novels of the 20th century.</review>
                </book>
                <book>
                    <author>Ralls, Kim</author>
                    <title>Midnight Rain</title>
                    <review>Some review.</review>
                </book>
                <secondcategory>
                    <paper id="bk102">
                        <test2>Ralls, Kim</test2>
                        <description>A former architect battles corporate zombies,
                        an evil sorceress, and her own childhood to become queen of the world.
                        </description>
                    </paper>
                </secondcategory>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) messageField.get("catalog");

        // Check books array
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> books = (List<Map<String, Object>>) catalog.get("book");
        assertThat(books.size(), equalTo(2));

        // Check secondcategory
        @SuppressWarnings("unchecked")
        Map<String, Object> secondcategory = (Map<String, Object>) catalog.get("secondcategory");
        @SuppressWarnings("unchecked")
        Map<String, Object> paper = (Map<String, Object>) secondcategory.get("paper");
        assertThat(paper.get("id"), equalTo("bk102"));
        assertThat(paper.get("test2"), equalTo("Ralls, Kim"));
    }

    public void testXmlDecodeWithUtf16Encoding() throws Exception {
        String xml = """
            <?xml version="1.0" encoding="UTF-16"?>
            <catalog>
                <book>
                    <author>William H. Gaddis</author>
                    <title>The Recognitions</title>
                    <review>One of the great seminal American novels of the 20th century.</review>
                </book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) messageField.get("catalog");
        @SuppressWarnings("unchecked")
        Map<String, Object> book = (Map<String, Object>) catalog.get("book");

        assertThat(book.get("author"), equalTo("William H. Gaddis"));
        assertThat(book.get("title"), equalTo("The Recognitions"));
    }

    public void testBrokenXmlWithIgnoreFailureFalse() {
        String brokenXml = """
            <?xml version="1.0"?>
            <catalog>
                <book>
                    <author>William H. Gaddis</author>
                    <title>The Recognitions</title>
                    <review>One of the great seminal American novels of the 20th century.</review>
            </ook>
            catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", brokenXml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("contains invalid XML"));
    }

    public void testBrokenXmlWithIgnoreFailureTrue() throws Exception {
        String brokenXml = """
            <?xml version="1.0"?>
            <catalog>
                <book>
                    <author>William H. Gaddis</author>
                    <title>The Recognitions</title>
                    <review>One of the great seminal American novels of the 20th century.</review>
            </ook>
            catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, true, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", brokenXml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        // Should not throw exception and leave document unchanged
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo(brokenXml));
    }

    public void testFieldNotFound() {
        XmlProcessor processor = new XmlProcessor("tag", null, "nonexistent", "target", false, false, false, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        Exception exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("not present as part of path [nonexistent]"));
    }

    public void testFieldNotFoundWithIgnoreMissing() throws Exception {
        XmlProcessor processor = new XmlProcessor("tag", null, "nonexistent", "target", true, false, false, false);
        IngestDocument originalDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalDocument);

        processor.execute(ingestDocument);

        // Document should remain unchanged
        assertThat(ingestDocument.getSourceAndMetadata(), equalTo(originalDocument.getSourceAndMetadata()));
    }

    public void testNullValue() {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("field [field] is null"));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", true, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", null);
        IngestDocument originalDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        IngestDocument ingestDocument = new IngestDocument(originalDocument);

        processor.execute(ingestDocument);

        // Document should remain unchanged
        assertThat(ingestDocument.getSourceAndMetadata(), equalTo(originalDocument.getSourceAndMetadata()));
    }

    public void testNonStringValueWithIgnoreFailureFalse() {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", 123);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("field [field] is not a string"));
    }

    public void testNonStringValueWithIgnoreFailureTrue() throws Exception {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, true, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", 123);
        IngestDocument originalDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        IngestDocument ingestDocument = new IngestDocument(originalDocument);

        processor.execute(ingestDocument);

        // Document should remain unchanged
        assertThat(ingestDocument.getSourceAndMetadata(), equalTo(originalDocument.getSourceAndMetadata()));
    }

    public void testEmptyXml() throws Exception {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", "");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        // Empty XML should result in null target
        assertThat(ingestDocument.getFieldValue("target", Object.class), equalTo(null));
    }

    public void testWhitespaceOnlyXml() throws Exception {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", "   \n\t  ");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        // Whitespace-only XML should result in null target
        assertThat(ingestDocument.getFieldValue("target", Object.class), equalTo(null));
    }

    public void testSelfClosingTag() throws Exception {
        String xml = "<empty/>";

        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) ingestDocument.getFieldValue("target", Object.class);
        assertThat(result.get("empty"), equalTo(null));
    }

    public void testSelfClosingTagWithAttributes() throws Exception {
        String xml = "<empty id=\"123\" name=\"test\"/>";

        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) ingestDocument.getFieldValue("target", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> empty = (Map<String, Object>) result.get("empty");
        assertThat(empty.get("id"), equalTo("123"));
        assertThat(empty.get("name"), equalTo("test"));
    }

    public void testGetType() {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        assertThat(processor.getType(), equalTo("xml"));
    }

    public void testGetters() {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", true, true, true, false);
        assertThat(processor.getField(), equalTo("field"));
        assertThat(processor.getTargetField(), equalTo("target"));
        assertThat(processor.isIgnoreMissing(), equalTo(true));
    }

    public void testIgnoreEmptyValueEnabled() throws Exception {
        String xml = """
            <catalog>
                <book>
                    <author>William H. Gaddis</author>
                    <title></title>
                    <review>One of the great seminal American novels.</review>
                    <empty/>
                    <nested>
                        <empty_text>   </empty_text>
                        <valid_content>Some content</valid_content>
                    </nested>
                </book>
                <empty_book></empty_book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, true);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) messageField.get("catalog");
        @SuppressWarnings("unchecked")
        Map<String, Object> book = (Map<String, Object>) catalog.get("book");

        // Empty title should be filtered out
        assertThat(book.containsKey("title"), equalTo(false));
        // Empty element should be filtered out
        assertThat(book.containsKey("empty"), equalTo(false));
        // empty_book should be filtered out entirely
        assertThat(catalog.containsKey("empty_book"), equalTo(false));

        // Valid content should remain
        assertThat(book.get("author"), equalTo("William H. Gaddis"));
        assertThat(book.get("review"), equalTo("One of the great seminal American novels."));

        // Nested structure handling
        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) book.get("nested");
        assertThat(nested.containsKey("empty_text"), equalTo(false)); // Whitespace-only should be filtered
        assertThat(nested.get("valid_content"), equalTo("Some content"));
    }

    public void testIgnoreEmptyValueWithArrays() throws Exception {
        String xml = """
            <catalog>
                <book>
                    <title>Valid Book</title>
                </book>
                <book>
                    <title></title>
                </book>
                <book>
                    <title>Another Valid Book</title>
                </book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, true);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) messageField.get("catalog");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> books = (List<Map<String, Object>>) catalog.get("book");

        // Should have 2 books after filtering out the one with empty title
        assertThat(books.size(), equalTo(2));
        assertThat(books.get(0).get("title"), equalTo("Valid Book"));
        assertThat(books.get(1).get("title"), equalTo("Another Valid Book"));
    }

    public void testIgnoreEmptyValueDisabled() throws Exception {
        String xml = """
            <catalog>
                <book>
                    <author>William H. Gaddis</author>
                    <title></title>
                    <empty/>
                </book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "message", "message", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("message", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> messageField = (Map<String, Object>) ingestDocument.getFieldValue("message", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) messageField.get("catalog");
        @SuppressWarnings("unchecked")
        Map<String, Object> book = (Map<String, Object>) catalog.get("book");

        // Empty values should remain when ignore_empty_value is false
        assertThat(book.containsKey("title"), equalTo(true));
        assertThat(book.get("title"), equalTo(null));  // Empty elements are parsed as null
        assertThat(book.containsKey("empty"), equalTo(true));
        assertThat(book.get("empty"), equalTo(null));
        assertThat(book.get("author"), equalTo("William H. Gaddis"));
    }

    public void testGettersWithIgnoreEmptyValue() {
        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", true, true, true, true);
        assertThat(processor.getField(), equalTo("field"));
        assertThat(processor.getTargetField(), equalTo("target"));
        assertThat(processor.isIgnoreMissing(), equalTo(true));
        assertThat(processor.isIgnoreEmptyValue(), equalTo(true));
    }

    public void testElementsWithAttributesAndTextContent() throws Exception {
        String xml = """
            <catalog version="1.0">
                <book id="123" isbn="978-0-684-80335-9">
                    <title lang="en">The Recognitions</title>
                    <author nationality="American">William H. Gaddis</author>
                    <price currency="USD">29.99</price>
                </book>
            </catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, false, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) ingestDocument.getFieldValue("target", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) result.get("catalog");
        @SuppressWarnings("unchecked")
        Map<String, Object> book = (Map<String, Object>) catalog.get("book");
        @SuppressWarnings("unchecked")
        Map<String, Object> title = (Map<String, Object>) book.get("title");
        @SuppressWarnings("unchecked")
        Map<String, Object> author = (Map<String, Object>) book.get("author");
        @SuppressWarnings("unchecked")
        Map<String, Object> price = (Map<String, Object>) book.get("price");

        // Test catalog with attributes only
        assertThat(catalog.get("version"), equalTo("1.0"));

        // Test book with attributes only (no text content)
        assertThat(book.get("id"), equalTo("123"));
        assertThat(book.get("isbn"), equalTo("978-0-684-80335-9"));

        // Test elements with both attributes and text content (should use #text key)
        assertThat(title.get("lang"), equalTo("en"));
        assertThat(title.get("#text"), equalTo("The Recognitions"));

        assertThat(author.get("nationality"), equalTo("American"));
        assertThat(author.get("#text"), equalTo("William H. Gaddis"));

        assertThat(price.get("currency"), equalTo("USD"));
        assertThat(price.get("#text"), equalTo("29.99"));
    }

    public void testMixedAttributesAndTextWithToLower() throws Exception {
        String xml = """
            <Catalog Version="1.0">
                <Book ID="123">
                    <Title Lang="EN">The Recognitions</Title>
                    <Author Nationality="AMERICAN">William H. Gaddis</Author>
                </Book>
            </Catalog>""";

        XmlProcessor processor = new XmlProcessor("tag", null, "field", "target", false, false, true, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", xml);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) ingestDocument.getFieldValue("target", Object.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> catalog = (Map<String, Object>) result.get("catalog");
        @SuppressWarnings("unchecked")
        Map<String, Object> book = (Map<String, Object>) catalog.get("book");
        @SuppressWarnings("unchecked")
        Map<String, Object> title = (Map<String, Object>) book.get("title");
        @SuppressWarnings("unchecked")
        Map<String, Object> author = (Map<String, Object>) book.get("author");

        // Test that element names are converted to lowercase
        assertThat(catalog.get("version"), equalTo("1.0"));
        assertThat(book.get("id"), equalTo("123"));

        // Test that attribute names are converted to lowercase but values remain unchanged
        assertThat(title.get("lang"), equalTo("EN"));
        assertThat(title.get("#text"), equalTo("The Recognitions"));

        assertThat(author.get("nationality"), equalTo("AMERICAN"));
        assertThat(author.get("#text"), equalTo("William H. Gaddis"));
    }
}
