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

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link XmlProcessor}. These tests ensure feature parity and test coverage.
 */
@SuppressWarnings("unchecked")
public class XmlProcessorTests extends ESTestCase {

    private static final String XML_FIELD = "xmldata";
    private static final String TARGET_FIELD = "data";

    private static IngestDocument createTestIngestDocument(String xml) {
        return new IngestDocument("_index", "_id", 1, null, null, new HashMap<>(Map.of(XML_FIELD, xml)));
    }

    private static XmlProcessor createTestProcessor(Map<String, Object> config) {
        config.putIfAbsent("field", XML_FIELD);
        config.putIfAbsent("target_field", TARGET_FIELD);

        XmlProcessor.Factory factory = new XmlProcessor.Factory();
        try {
            return factory.create(null, "_tag", null, config, null);
        } catch (Exception e) {
            fail("Failed to create XmlProcessor: " + e.getMessage());
            return null; // This line will never be reached, but is needed to satisfy the compiler
        }
    }

    /**
     * Test parsing standard XML with attributes.
     */
    public void testParseStandardXml() {
        String xml = "<foo key=\"value\"/>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");
        assertThat(foo.get("key"), equalTo("value"));
    }

    /**
     * Test parsing XML with array elements (multiple elements with same name).
     */
    public void testParseXmlWithArrayValue() {
        String xml = "<foo><key>value1</key><key>value2</key></foo>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");
        List<Object> keyValues = (List<Object>) foo.get("key");
        assertThat(keyValues.size(), equalTo(2));

        // The values might be nested inside their own lists
        Object firstValue = keyValues.get(0);
        assertThat(firstValue, equalTo("value1"));

        Object secondValue = keyValues.get(1);
        assertThat(secondValue, equalTo("value2"));
    }

    /**
     * Test parsing XML with nested elements.
     */
    public void testParseXmlWithNestedElements() {
        String xml = "<foo><key1><key2>value</key2></key1></foo>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");

        Map<String, Object> key1Map = (Map<String, Object>) foo.get("key1");
        assertThat(key1Map.size(), equalTo(1));

        String key2Value = (String) key1Map.get("key2");
        assertThat(key2Value, equalTo("value"));
    }

    /**
     * Test parsing XML in a single item array.
     */
    public void testParseXmlInSingleItemArray() {
        String xml = "<foo bar=\"baz\"/>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");
        assertThat(foo.get("bar"), equalTo("baz"));
    }

    /**
     * Test extracting a single element using XPath.
     */
    public void testXPathSingleElementExtraction() {
        String xml = "<foo><bar>hello</bar><baz>world</baz></foo>";

        Map<String, String> xpathMap = Map.of("/foo/bar/text()", "bar_content");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        // Get the XPath result
        Object barContent = ingestDocument.getFieldValue("bar_content", Object.class);
        assertNotNull(barContent);
        assertEquals("hello", barContent);

        // Verify that the full parsed XML is also available
        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");
        assertNotNull(foo);
        assertThat(foo.get("bar"), equalTo("hello"));
        assertThat(foo.get("baz"), equalTo("world"));
    }

    /**
     * Test extracting multiple elements using XPath.
     */
    public void testXPathMultipleElementsExtraction() {
        String xml = "<foo><bar>first</bar><bar>second</bar><bar>third</bar></foo>";

        Map<String, String> xpathMap = Map.of("/foo/bar", "all_bars");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        List<String> allBars = ingestDocument.getFieldValue("all_bars", List.class);

        assertNotNull(allBars);
        assertThat(allBars.size(), equalTo(3));
        assertThat(allBars.get(0), equalTo("first"));
        assertThat(allBars.get(1), equalTo("second"));
        assertThat(allBars.get(2), equalTo("third"));
    }

    /**
     * Test extracting attributes using XPath.
     */
    public void testXPathAttributeExtraction() {
        String xml = "<foo><bar id=\"123\" type=\"test\">content</bar></foo>";

        Map<String, String> xpathMap = new HashMap<>();
        xpathMap.put("/foo/bar/@id", "bar_id");
        xpathMap.put("/foo/bar/@type", "bar_type");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        String barId = ingestDocument.getFieldValue("bar_id", String.class);
        assertNotNull(barId);
        assertThat(barId, equalTo("123"));

        String barType = ingestDocument.getFieldValue("bar_type", String.class);
        assertNotNull(barType);
        assertThat(barType, equalTo("test"));
    }

    /**
     * Test extracting elements with namespaces using XPath.
     */
    public void testXPathNamespacedExtraction() {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<root xmlns:myns=\"http://example.org/ns1\">"
            + "  <myns:element>namespace-value</myns:element>"
            + "  <regular>regular-value</regular>"
            + "</root>";

        Map<String, String> namespaces = Map.of("myns", "http://example.org/ns1");
        Map<String, String> xpathMap = Map.of("//myns:element/text()", "ns_value");

        Map<String, Object> config = new HashMap<>();
        config.put("xpath", xpathMap);
        config.put("namespaces", namespaces);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        String nsValue = ingestDocument.getFieldValue("ns_value", String.class);
        assertNotNull(nsValue);
        assertThat(nsValue, equalTo("namespace-value"));
    }

    /**
     * Test parsing XML with mixed content (text and elements mixed together).
     */
    public void testParseXmlWithMixedContent() {
        String xml = "<foo>This text is <b>bold</b> and this is <i>italic</i>!</foo>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");

        assertNotNull(foo.get("b"));
        assertThat((String) foo.get("b"), equalTo("bold"));
        assertNotNull(foo.get("i"));
        assertThat((String) foo.get("i"), equalTo("italic"));
        assertNotNull(foo.get("#text"));
        assertThat((String) foo.get("#text"), equalTo("This text is  and this is !"));
    }

    /**
     * Test parsing XML with CDATA sections.
     */
    public void testParseXmlWithCDATA() {
        String xml = "<foo><![CDATA[This is CDATA content with <tags> that shouldn't be parsed!]]></foo>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Object content = data.get("foo");

        assertNotNull(content);
        assertThat(content, equalTo("This is CDATA content with <tags> that shouldn't be parsed!"));
    }

    /**
     * Test parsing XML with numeric data.
     */
    public void testParseXmlWithNumericData() {
        String xml = "<foo><count>123</count><price>99.95</price><active>true</active></foo>";

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");

        assertThat((String) foo.get("count"), equalTo("123"));
        assertThat((String) foo.get("price"), equalTo("99.95"));
        assertThat((String) foo.get("active"), equalTo("true"));
    }

    /**
     * Test parsing XML with force_array option enabled.
     */
    public void testParseXmlWithForceArray() {
        String xml = "<foo><bar>single_value</bar></foo>";

        Map<String, Object> config = new HashMap<>();
        config.put("force_array", true); // Enable force_array option
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = (Map<String, Object>) ingestDocument.getFieldValue(TARGET_FIELD, Object.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");

        // With force_array=true, even single values should be in arrays
        Object barValue = foo.get("bar");
        assertNotNull(barValue);
        assertTrue("Expected bar value to be a List with force_array=true", barValue instanceof List);

        List<String> barList = (List<String>) barValue;
        assertThat(barList.size(), equalTo(1));
        assertThat(barList.get(0), equalTo("single_value"));
    }

    /**
     * Test extracting multiple elements using multiple XPath expressions.
     * Tests that multiple XPath expressions can be used simultaneously.
     */
    public void testMultipleXPathExpressions() {
        String xml = "<root>"
            + "  <person id=\"1\"><n>John</n><age>30</age></person>"
            + "  <person id=\"2\"><n>Jane</n><age>25</age></person>"
            + "</root>";

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

        assertTrue("first_person_name field should exist", ingestDocument.hasField("first_person_name"));
        assertTrue("second_person_name field should exist", ingestDocument.hasField("second_person_name"));
        assertTrue("person_ids field should exist", ingestDocument.hasField("person_ids"));

        Object firstName = ingestDocument.getFieldValue("first_person_name", Object.class);
        assertEquals("John", firstName);

        Object secondName = ingestDocument.getFieldValue("second_person_name", Object.class);
        assertEquals("Jane", secondName);

        Object personIdsObj = ingestDocument.getFieldValue("person_ids", Object.class);
        assertTrue("person_ids should be a List", personIdsObj instanceof List);
        List<?> personIds = (List<?>) personIdsObj;
        assertEquals("Should have 2 person IDs", 2, personIds.size());
        assertEquals("First person ID should be '1'", "1", personIds.get(0));
        assertEquals("Second person ID should be '2'", "2", personIds.get(1));

        assertTrue("Target field should exist", ingestDocument.hasField(TARGET_FIELD));
    }

    /**
     * Test handling of invalid XML with ignoreFailure=false.
     */
    public void testInvalidXml() {
        String xml = "<foo><unclosed>"; // Invalid XML missing closing tag

        Map<String, Object> config = new HashMap<>();
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));

        assertTrue(
            "Error message should indicate XML is invalid",
            exception.getMessage().contains("invalid XML") || exception.getCause().getMessage().contains("XML")
        );
    }

    /**
     * Test handling of invalid XML with ignoreFailure=true.
     * Note: The ignore_failure parameter is handled by the framework's OnFailureProcessor wrapper.
     * When calling the processor directly (as in tests), exceptions are still thrown.
     * This test verifies that the processor itself properly reports XML parsing errors.
     */
    public void testInvalidXmlWithIgnoreFailure() {
        String xml = "<foo><unclosed>"; // Invalid XML missing closing tag

        Map<String, Object> config = new HashMap<>();
        config.put("ignore_failure", true);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        // Even with ignore_failure=true, calling the processor directly still throws exceptions
        // The framework's OnFailureProcessor wrapper handles the ignore_failure behavior in production
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));

        assertTrue(
            "Error message should indicate XML is invalid",
            exception.getMessage().contains("invalid XML") || exception.getCause().getMessage().contains("XML")
        );
    }

    /**
     * Test the store_xml=false option to not store parsed XML in target field.
     */
    public void testNoStoreXml() {
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
        assertNotNull(barContent);
        assertThat(barContent, equalTo("value"));

        // Verify the target field was not created
        assertFalse(ingestDocument.hasField(TARGET_FIELD));
    }

    /**
     * Test the to_lower option for converting field names to lowercase.
     */
    public void testToLower() {
        String xml = "<FOO><BAR>value</BAR></FOO>";

        Map<String, Object> config = new HashMap<>();
        config.put("to_lower", true); // Enable to_lower option
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        // Verify field names are lowercase
        Map<String, Object> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);
        assertTrue(data.containsKey("foo"));
        assertFalse(data.containsKey("FOO"));

        Map<String, Object> foo = (Map<String, Object>) data.get("foo");
        assertTrue(foo.containsKey("bar"));
        assertFalse(foo.containsKey("BAR"));
        assertThat(foo.get("bar"), equalTo("value"));
    }

    /**
     * Test the ignore_missing option when field is missing.
     */
    public void testIgnoreMissing() {
        String xmlField = "nonexistent_field";

        Map<String, Object> config = new HashMap<>();
        config.put("field", xmlField);
        config.put("ignore_missing", true); // Enable ignore_missing option
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = new IngestDocument("_index", "_id", 1, null, null, new HashMap<>(Map.of()));
        processor.execute(ingestDocument);

        assertFalse("Target field should not be created when source field is missing", ingestDocument.hasField(TARGET_FIELD));

        // With ignoreMissing=false
        config.put("ignore_missing", false);
        XmlProcessor failingProcessor = createTestProcessor(config);

        // This should throw an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> failingProcessor.execute(ingestDocument));

        assertTrue(exception.getMessage().contains("not present as part of path"));
    }

    /**
     * Test that remove_empty_values correctly filters out empty values from arrays and mixed content.
     */
    public void testRemoveEmptyValues() {
        // XML with mixed empty and non-empty elements, including array elements with mixed empty/non-empty values
        String xml = "<root>"
            + "  <empty></empty>"
            + "  <blank>   </blank>"
            + "  <valid>content</valid>"
            + "  <nested><empty/><valid>nested-content</valid></nested>"
            + "  <items>"
            + "    <item>first</item>"
            + "    <item></item>"
            + "    <item>third</item>"
            + "    <item>   </item>"
            + "    <item>fifth</item>"
            + "  </items>"
            + "  <mixed>Text with <empty></empty> and <valid>content</valid></mixed>"
            + "</root>";

        Map<String, Object> config = new HashMap<>();
        config.put("remove_empty_values", true);
        XmlProcessor processor = createTestProcessor(config);

        IngestDocument ingestDocument = createTestIngestDocument(xml);
        processor.execute(ingestDocument);

        Map<String, Object> result = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);
        Map<String, Object> root = (Map<String, Object>) result.get("root");

        // Check empty elements are filtered
        assertFalse("Empty element should be filtered out", root.containsKey("empty"));
        assertFalse("Blank element should be filtered out", root.containsKey("blank"));

        // Check valid elements are preserved
        assertTrue("Valid element should be preserved", root.containsKey("valid"));
        assertEquals("content", root.get("valid"));

        // Check nested structure filtering
        Map<String, Object> nested = (Map<String, Object>) root.get("nested");
        assertNotNull("Nested element should be preserved", nested);
        assertFalse("Empty nested element should be filtered", nested.containsKey("empty"));
        assertEquals("nested-content", nested.get("valid"));

        // Check array with mixed empty/non-empty values
        Map<String, Object> items = (Map<String, Object>) root.get("items");
        assertNotNull("Items object should be preserved", items);
        List<String> itemList = (List<String>) items.get("item");
        assertNotNull("Item array should be preserved", itemList);
        assertEquals("Array should contain only non-empty items", 3, itemList.size());
        assertEquals("first", itemList.get(0));
        assertEquals("third", itemList.get(1));
        assertEquals("fifth", itemList.get(2));

        // Check mixed content handling
        Map<String, Object> mixed = (Map<String, Object>) root.get("mixed");
        assertNotNull("Mixed content should be preserved", mixed);
        assertFalse("Empty element in mixed content should be filtered", mixed.containsKey("empty"));
        assertTrue("Valid element in mixed content should be preserved", mixed.containsKey("valid"));
        assertEquals("content", mixed.get("valid"));
        assertEquals("Text with  and", mixed.get("#text"));
    }

    /**
     * Test parsing with strict mode option.
     */
    public void testStrictParsing() {
        String xml = "<foo><bar>valid</bar></foo>";

        Map<String, Object> config = new HashMap<>();
        config.put("strict_parsing", true);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");
        assertThat(foo.get("bar"), equalTo("valid"));

        // Test with invalid XML in strict mode
        String invalidXml = "<foo><invalid & xml</foo>";
        IngestDocument invalidDocument = createTestIngestDocument(invalidXml);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(invalidDocument));

        assertTrue(
            "Error message should indicate XML is invalid",
            exception.getMessage().contains("invalid XML") || exception.getCause().getMessage().contains("XML")
        );
    }

    /**
     * Test parsing XML with remove_namespaces option.
     */
    public void testRemoveNamespaces() {
        String xml = "<foo xmlns:ns=\"http://example.org/ns\"><ns:bar>value</ns:bar></foo>";

        Map<String, Object> config = new HashMap<>();
        config.put("remove_namespaces", true);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");

        assertTrue("Element without namespace should be present", foo.containsKey("bar"));
        assertThat(foo.get("bar"), equalTo("value"));

        // Now test with removeNamespaces=false
        IngestDocument ingestDocument2 = createTestIngestDocument(xml);

        config.put("remove_namespaces", false);
        XmlProcessor processor2 = createTestProcessor(config);
        processor2.execute(ingestDocument2);

        Map<String, Object> data2 = ingestDocument2.getFieldValue(TARGET_FIELD, Map.class);
        Map<String, Object> foo2 = (Map<String, Object>) data2.get("foo");

        // With removeNamespaces=false, the "ns:" prefix should be preserved
        assertTrue("Element should be accessible with namespace prefix", foo2.containsKey("ns:bar"));
        assertThat(foo2.get("ns:bar"), equalTo("value"));
    }

    /**
     * Test the force_content option.
     */
    public void testForceContent() {
        String xml = "<foo>simple text</foo>";

        Map<String, Object> config = new HashMap<>();
        config.put("force_content", true);
        XmlProcessor processor = createTestProcessor(config);
        IngestDocument ingestDocument = createTestIngestDocument(xml);

        processor.execute(ingestDocument);

        Map<String, Object> data = ingestDocument.getFieldValue(TARGET_FIELD, Map.class);
        Map<String, Object> foo = (Map<String, Object>) data.get("foo");

        // With forceContent=true, the text should be in a #text field
        assertTrue("Text content should be in #text field", foo.containsKey("#text"));
        assertThat(foo.get("#text"), equalTo("simple text"));

        // Now test with forceContent=false
        config.put("force_content", false);
        XmlProcessor processor2 = createTestProcessor(config);
        IngestDocument ingestDocument2 = createTestIngestDocument(xml);

        processor2.execute(ingestDocument2);

        Map<String, Object> data2 = ingestDocument2.getFieldValue(TARGET_FIELD, Map.class);

        // With forceContent=false, the text should be directly assigned to the element
        assertThat(data2.get("foo"), equalTo("simple text"));
    }
}
