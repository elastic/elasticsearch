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
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link XmlProcessor}. These tests ensure feature parity and test coverage.
 */
public class XmlProcessorTests extends ESTestCase {

    private static final String XML_FIELD = "xmldata";
    private static final String TARGET_FIELD = "data";

    private static IngestDocument createTestIngestDocument(String xml) {
        return new IngestDocument("_index", "_id", 1, null, null, new HashMap<>(Map.of(XML_FIELD, xml)));
    }

    private static XmlProcessor createTestProcessor(Map<String, Object> config) throws Exception {
        config.putIfAbsent("field", XML_FIELD);
        config.putIfAbsent("target_field", TARGET_FIELD);

        XmlProcessor.Factory factory = new XmlProcessor.Factory();
        return factory.create(null, "_tag", null, config, null);
    }

    /**
     * Test parsing standard XML structure.
     */
    public void testParseStandardXml() throws Exception {
        String xml = "<foo key=\"value\"/>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("key", "value"));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test parsing XML with array elements (multiple elements with same name).
     */
    public void testParseXmlWithArrayValue() throws Exception {
        String xml = """
            <foo>
              <key>value1</key>
              <key>value2</key>
            </foo>""";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("key", List.of("value1", "value2")));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test parsing XML with nested elements.
     */
    public void testParseXmlWithNestedElements() throws Exception {
        String xml = """
            <foo>
              <key1>
                <key2>value</key2>
              </key1>
            </foo>""";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("key1", Map.of("key2", "value")));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test parsing XML in a single item array.
     */
    public void testParseXmlInSingleItemArray() throws Exception {
        String xml = "<foo bar=\"baz\"/>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("bar", "baz"));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test extracting a single element using XPath.
     */
    public void testXPathSingleElementExtraction() throws Exception {
        String xml = """
            <foo>
              <bar>hello</bar>
              <baz>world</baz>
            </foo>""";

        Map<String, String> xpathMap = Map.of("/foo/bar/text()", "bar_content");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        // Get the XPath result
        Object barContent = ingestDocument.getFieldValue("bar_content", Object.class);
        assertThat(barContent, equalTo("hello"));

        // Verify that the full parsed XML is also available
        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("bar", "hello", "baz", "world"));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test extracting multiple elements using XPath.
     */
    public void testXPathMultipleElementsExtraction() throws Exception {
        String xml = """
            <foo>
              <bar>first</bar>
              <bar>second</bar>
              <bar>third</bar>
            </foo>""";

        Map<String, String> xpathMap = Map.of("/foo/bar", "all_bars");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        @SuppressWarnings("unchecked")
        List<String> allBars = (List<String>) ingestDocument.getFieldValue("all_bars", List.class);
        List<String> expectedBars = List.of("first", "second", "third");
        assertThat(allBars, equalTo(expectedBars));
    }

    /**
     * Test extracting attributes using XPath.
     */
    public void testXPathAttributeExtraction() throws Exception {
        String xml = """
            <foo>
              <bar id="123" type="test">content</bar>
            </foo>""";

        Map<String, String> xpathMap = new HashMap<>();
        xpathMap.put("/foo/bar/@id", "bar_id");
        xpathMap.put("/foo/bar/@type", "bar_type");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        String barId = ingestDocument.getFieldValue("bar_id", String.class);
        assertThat(barId, equalTo("123"));

        String barType = ingestDocument.getFieldValue("bar_type", String.class);
        assertThat(barType, equalTo("test"));
    }

    /**
     * Test extracting elements with namespaces using XPath.
     */
    public void testXPathNamespacedExtraction() throws Exception {
        String xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <root xmlns:myns="http://example.org/ns1">
              <myns:element>namespace-value</myns:element>
              <regular>regular-value</regular>
            </root>""";

        Map<String, String> namespaces = Map.of("myns", "http://example.org/ns1");
        Map<String, String> xpathMap = Map.of("//myns:element/text()", "ns_value");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        config.put("namespaces", namespaces);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        String nsValue = ingestDocument.getFieldValue("ns_value", String.class);
        assertThat(nsValue, equalTo("namespace-value"));
    }

    /**
     * Test parsing XML with mixed content (text and elements mixed together).
     */
    public void testParseXmlWithMixedContent() throws Exception {
        String xml = """
            <foo>
              This text is <b>bold</b> and this is <i>italic</i>!
            </foo>""";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("b", "bold", "i", "italic", "#text", "This text is  and this is !"));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test parsing XML with CDATA sections.
     */
    public void testParseXmlWithCDATA() throws Exception {
        String xml = "<foo><![CDATA[This is CDATA content with <tags> that shouldn't be parsed!]]></foo>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", "This is CDATA content with <tags> that shouldn't be parsed!");
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test parsing XML with numeric data.
     */
    public void testParseXmlWithNumericData() throws Exception {
        String xml = """
            <foo>
              <count>123</count>
              <price>99.95</price>
              <active>true</active>
            </foo>""";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("count", "123", "price", "99.95", "active", "true"));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test parsing XML with force_array option enabled.
     */
    public void testParseXmlWithForceArray() throws Exception {
        String xml = "<foo><bar>single_value</bar></foo>";

        Map<String, Object> config = new HashMap<>();
        config.put("force_array", true); // Enable force_array option
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("bar", List.of("single_value")));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test extracting multiple elements using multiple XPath expressions.
     * Tests that multiple XPath expressions can be used simultaneously.
     */
    public void testMultipleXPathExpressions() throws Exception {
        String xml = """
            <root>
              <person id="1">
                <n>John</n>
                <age>30</age>
              </person>
              <person id="2">
                <n>Jane</n>
                <age>25</age>
              </person>
            </root>""";

        // Configure multiple XPath expressions
        Map<String, String> xpathMap = new HashMap<>();
        xpathMap.put("/root/person[1]/n/text()", "first_person_name");
        xpathMap.put("/root/person[2]/n/text()", "second_person_name");
        xpathMap.put("/root/person/@id", "person_ids");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        // Verify XPath results
        Object firstName = ingestDocument.getFieldValue("first_person_name", Object.class);
        assertThat(firstName, equalTo("John"));

        Object secondName = ingestDocument.getFieldValue("second_person_name", Object.class);
        assertThat(secondName, equalTo("Jane"));

        List<?> personIds = ingestDocument.getFieldValue("person_ids", List.class);
        assertThat(personIds, equalTo(List.of("1", "2")));

        // Verify that the target field was also created (since storeXml defaults to true)
        assertThat(ingestDocument.hasField(TARGET_FIELD), equalTo(true));
    }

    /**
     * Test handling of invalid XML with ignoreFailure=false.
     */
    public void testInvalidXml() throws Exception {
        String xml = "<foo><unclosed>"; // Invalid XML missing closing tag

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));

        assertThat(exception.getMessage(), containsString("invalid XML"));
    }

    /**
     * Test handling of invalid XML with ignoreFailure=true.
     * Note: The ignore_failure parameter is handled by the framework's OnFailureProcessor wrapper.
     * When calling the processor directly (as in tests), exceptions are still thrown.
     * This test verifies that the processor itself properly reports XML parsing errors.
     */
    public void testInvalidXmlWithIgnoreFailure() throws Exception {
        String xml = "<foo><unclosed>"; // Invalid XML missing closing tag

        Map<String, Object> config = new HashMap<>();
        config.put("ignore_failure", true);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        // Even with ignore_failure=true, calling the processor directly still throws exceptions
        // The framework's OnFailureProcessor wrapper handles the ignore_failure behavior in production
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));

        assertThat(exception.getMessage(), containsString("invalid XML"));
    }

    /**
     * Test the store_xml=false option to not store parsed XML in target field.
     */
    public void testNoStoreXml() throws Exception {
        String xml = "<foo><bar>value</bar></foo>";

        // Set up XPath to extract value but don't store XML
        Map<String, String> xpathMap = Map.of("/foo/bar/text()", "bar_content");

        Map<String, Object> config = new HashMap<>();
        config.put("store_xml", false); // Do not store XML in target field
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        // Verify XPath result is stored
        String barContent = ingestDocument.getFieldValue("bar_content", String.class);
        assertThat(barContent, equalTo("value"));

        // Verify the target field was not created
        assertThat(ingestDocument.hasField(TARGET_FIELD), is(false));
    }

    /**
     * Test the to_lower option for converting field names to lowercase.
     */
    public void testToLower() throws Exception {
        String xml = "<FOO><BAR>value</BAR></FOO>";

        Map<String, Object> config = new HashMap<>();
        config.put("to_lower", true); // Enable to_lower option
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        // Verify field names are lowercase
        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of("foo", Map.of("bar", "value"));
        assertThat(data, equalTo(expectedData));
    }

    /**
     * Test the ignore_missing option when field is missing.
     */
    public void testIgnoreMissing() throws Exception {
        String xmlField = "nonexistent_field";

        Map<String, Object> config = new HashMap<>();
        config.put("field", xmlField);
        config.put("ignore_missing", true); // Enable ignore_missing option
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = new IngestDocument("_index", "_id", 1, null, null, new HashMap<>(Map.of()));
        processor.execute(ingestDocument);

        assertThat("Target field should not be created when source field is missing", ingestDocument.hasField(TARGET_FIELD), is(false));

        // With ignoreMissing=false
        config.put("ignore_missing", false);
        XmlProcessor failingProcessor = createTestProcessor(config);

        // This should throw an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> failingProcessor.execute(ingestDocument));

        assertThat(exception.getMessage(), containsString("not present as part of path"));
    }

    /**
     * Test that remove_empty_values correctly filters out empty values from arrays and mixed content.
     */
    public void testRemoveEmptyValues() throws Exception {
        // XML with mixed empty and non-empty elements, including array elements with mixed empty/non-empty values
        String xml = """
            <root>
              <empty></empty>
              <blank>   </blank>
              <valid>content</valid>
              <nested>
                <empty/>
                <valid>nested-content</valid>
              </nested>
              <items>
                <item>first</item>
                <item></item>
                <item>third</item>
                <item>   </item>
                <item>fifth</item>
              </items>
              <mixed>Text with <empty></empty> and <valid>content</valid></mixed>
            </root>""";

        Map<String, Object> config = new HashMap<>();
        config.put("remove_empty_values", true);
        XmlProcessor processor = createTestProcessor(config);

        IngestDocument ingestDocument = createTestIngestDocument(xml);
        processor.execute(ingestDocument);

        Map<?, ?> result = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedData = Map.of(
            "root",
            Map.of(
                "valid",
                "content",
                "nested",
                Map.of("valid", "nested-content"),
                "items",
                Map.of("item", List.of("first", "third", "fifth")),
                "mixed",
                Map.of("valid", "content", "#text", "Text with  and")
            )
        );

        assertThat(result, equalTo(expectedData));
    }

    /**
     * Test parsing XML with remove_namespaces option.
     */
    public void testRemoveNamespaces() throws Exception {
        String xml = """
            <foo xmlns:ns="http://example.org/ns">
              <ns:bar>value</ns:bar>
            </foo>""";

        Map<String, Object> config = new HashMap<>();
        config.put("remove_namespaces", true);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedDataWithoutNs = Map.of("foo", Map.of("bar", "value"));
        assertThat(data, equalTo(expectedDataWithoutNs));

        // Now test with removeNamespaces=false
        IngestDocument ingestDocument2 = createTestIngestDocument(xml);

        config.put("remove_namespaces", false);
        XmlProcessor processor2 = createTestProcessor(config);
        processor2.execute(ingestDocument2);

        Map<?, ?> data2 = ingestDocument2.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedDataWithNs = Map.of("foo", Map.of("xmlns:ns", "http://example.org/ns", "ns:bar", "value"));
        assertThat(data2, equalTo(expectedDataWithNs));
    }

    /**
     * Test the force_content option.
     */
    public void testForceContent() throws Exception {
        String xml = "<foo>simple text</foo>";

        Map<String, Object> config = new HashMap<>();
        config.put("force_content", true);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<?, ?> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedDataWithForceContent = Map.of("foo", Map.of("#text", "simple text"));
        assertThat(data, equalTo(expectedDataWithForceContent));

        // Now test with forceContent=false
        config.put("force_content", false);
        XmlProcessor processor2 = createTestProcessor(config);
        IngestDocument ingestDocument2 = createTestIngestDocument(xml);

        processor2.execute(ingestDocument2);

        Map<?, ?> data2 = ingestDocument2.getFieldValue(TARGET_FIELD, Map.class);

        Map<String, Object> expectedDataWithoutForceContent = Map.of("foo", "simple text");
        assertThat(data2, equalTo(expectedDataWithoutForceContent));
    }
}
