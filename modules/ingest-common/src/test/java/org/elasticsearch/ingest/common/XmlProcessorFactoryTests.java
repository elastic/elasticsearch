/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class XmlProcessorFactoryTests extends ESTestCase {

    private static final String DEFAULT_FIELD = "field1";
    private static final String DEFAULT_TARGET_FIELD = "target";

    /**
     * Creates a new XmlProcessor.Factory instance for testing.
     */
    private XmlProcessor.Factory createFactory() {
        return new XmlProcessor.Factory();
    }

    /**
     * Creates a processor with the default factory and given configuration.
     * This validates that all configuration parameters are consumed during processor creation.
     */
    private XmlProcessor createProcessor(Map<String, Object> config) throws Exception {
        XmlProcessor.Factory factory = createFactory();
        String processorTag = randomAlphaOfLength(10);

        // Make a copy of the config to avoid modifying the original
        Map<String, Object> configCopy = new HashMap<>(config);

        // Create the processor (this should consume config parameters)
        XmlProcessor processor = factory.create(null, processorTag, null, configCopy, null);

        // Validate that all configuration parameters were consumed
        assertThat(configCopy, anEmptyMap());

        return processor;
    }

    /**
     * Helper to expect processor creation failure with specific message.
     */
    private void expectCreationFailure(Map<String, Object> config, Class<? extends Exception> exceptionClass, String expectedMessage) {
        XmlProcessor.Factory factory = createFactory();
        String processorTag = randomAlphaOfLength(10);

        // Make a mutable copy since Map.of creates immutable maps
        Map<String, Object> configCopy = new HashMap<>(config);

        Exception exception = expectThrows(exceptionClass, () -> factory.create(null, processorTag, null, configCopy, null));
        assertThat(exception.getMessage(), equalTo(expectedMessage));
    }

    /**
     * Tests processor creation with various configurations.
     */
    public void testCreate() throws Exception {
        Map<String, Object> config = Map.of(
            "field",
            DEFAULT_FIELD,
            "target_field",
            DEFAULT_TARGET_FIELD,
            "ignore_missing",
            true,
            "to_lower",
            true,
            "remove_empty_values",
            true
        );

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getTargetField(), equalTo(DEFAULT_TARGET_FIELD));
        assertThat(processor.isIgnoreMissing(), equalTo(true));
        assertThat(processor.isRemoveEmptyValues(), equalTo(true));
    }

    public void testCreateWithDefaults() throws Exception {
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD);
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getTargetField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isIgnoreMissing(), equalTo(false));
        assertThat(processor.isRemoveEmptyValues(), equalTo(false));
    }

    public void testCreateMissingField() throws Exception {
        Map<String, Object> config = Map.of(); // Empty config - no field specified
        expectCreationFailure(config, ElasticsearchParseException.class, "[field] required property is missing");
    }

    public void testCreateWithRemoveEmptyValuesOnly() throws Exception {
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "remove_empty_values", true);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isRemoveEmptyValues(), equalTo(true));
        assertThat(processor.isIgnoreMissing(), equalTo(false)); // other flags should remain default
    }

    public void testCreateWithXPath() throws Exception {
        Map<String, String> xpathConfig = Map.of("//author/text()", "author_field", "//title/@lang", "language_field");
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "xpath", xpathConfig);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
    }

    public void testCreateWithInvalidXPathConfig() throws Exception {
        Map<String, Object> config = Map.of(
            "field",
            DEFAULT_FIELD,
            "xpath",
            "invalid_string" // Should be a map
        );

        expectCreationFailure(config, ElasticsearchParseException.class, "[xpath] property isn't a map, but of type [java.lang.String]");
    }

    public void testCreateWithInvalidXPathTargetField() throws Exception {
        Map<String, Object> xpathConfig = new HashMap<>();
        xpathConfig.put("//author/text()", 123); // Should be string

        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "xpath", xpathConfig);

        expectCreationFailure(
            config,
            IllegalArgumentException.class,
            "XPath target field [//author/text()] must be a string, got [Integer]"
        );
    }

    public void testCreateWithNamespaces() throws Exception {
        Map<String, String> namespacesConfig = Map.of("book", "http://example.com/book", "author", "http://example.com/author");
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "namespaces", namespacesConfig);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getNamespaces(), equalTo(namespacesConfig));
    }

    public void testCreateWithInvalidNamespacesConfig() throws Exception {
        Map<String, Object> config = Map.of(
            "field",
            DEFAULT_FIELD,
            "namespaces",
            "invalid_string" // Should be a map
        );

        expectCreationFailure(

            config,

            ElasticsearchParseException.class,

            "[namespaces] property isn't a map, but of type [java.lang.String]"

        );
    }

    public void testCreateWithInvalidNamespaceURI() throws Exception {
        Map<String, Object> namespacesConfig = new HashMap<>();
        namespacesConfig.put("book", 123); // Should be string

        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "namespaces", namespacesConfig);

        expectCreationFailure(config, IllegalArgumentException.class, "Namespace prefix [book] must have a string URI, got [Integer]");
    }

    public void testCreateWithXPathAndNamespaces() throws Exception {
        Map<String, String> xpathConfig = Map.of("//book:author/text()", "author_field", "//book:title/@lang", "language_field");
        Map<String, String> namespacesConfig = Map.of("book", "http://example.com/book");
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "xpath", xpathConfig, "namespaces", namespacesConfig);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getNamespaces(), equalTo(namespacesConfig));
    }

    // Tests for individual boolean options

    public void testCreateWithStoreXmlFalse() throws Exception {
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "store_xml", false);
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isStoreXml(), equalTo(false));
    }

    public void testCreateWithRemoveNamespaces() throws Exception {
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "remove_namespaces", true);
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isRemoveNamespaces(), equalTo(true));
    }

    public void testCreateWithForceContent() throws Exception {
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "force_content", true);
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isForceContent(), equalTo(true));
    }

    public void testCreateWithForceArray() throws Exception {
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "force_array", true);
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isForceArray(), equalTo(true));
    }

    public void testCreateWithStrictParsing() throws Exception {
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "strict_parsing", true);
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getStrictParsing(), equalTo(true));
        assertThat(processor.isStrict(), equalTo(true));
    }

    public void testCreateWithMultipleOptions() throws Exception {
        Map<String, Object> config = Map.of(
            "field",
            DEFAULT_FIELD,
            "ignore_missing",
            true,
            "force_content",
            true,
            "force_array",
            true,
            "remove_namespaces",
            true
        );
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isIgnoreMissing(), equalTo(true));
        assertThat(processor.isForceContent(), equalTo(true));
        assertThat(processor.isForceArray(), equalTo(true));
        assertThat(processor.isRemoveNamespaces(), equalTo(true));
    }

    // Tests for XPath compilation errors (testing precompilation feature)

    public void testCreateWithInvalidXPathExpression() throws Exception {
        Map<String, String> xpathConfig = Map.of("invalid xpath ][", "target_field");
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "xpath", xpathConfig);

        XmlProcessor.Factory factory = createFactory();
        String processorTag = randomAlphaOfLength(10);

        // Make a mutable copy since Map.of creates immutable maps
        Map<String, Object> configCopy = new HashMap<>(config);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(null, processorTag, null, configCopy, null)
        );

        // Check that the error message contains the XPath expression and indicates it's invalid
        assertThat(exception.getMessage(), containsString("Invalid XPath expression [invalid xpath ][]"));
        assertThat(exception.getCause().getMessage(), containsString("javax.xml.transform.TransformerException"));
    }

    public void testCreateWithXPathUsingNamespacesWithoutConfiguration() throws Exception {
        Map<String, String> xpathConfig = Map.of("//book:title/text()", "title_field");
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "xpath", xpathConfig);

        expectCreationFailure(
            config,
            IllegalArgumentException.class,
            "Invalid XPath expression [//book:title/text()]: contains namespace prefixes but no namespace configuration provided"
        );
    }

    public void testConfigurationParametersAreProperlyRemoved() throws Exception {
        // Test that demonstrates configuration validation works when using production-like validation
        // This test verifies that all valid configuration parameters are consumed during processor creation

        Map<String, String> xpathConfig = Map.of("//test", "test_field");
        Map<String, Object> config = Map.of("field", DEFAULT_FIELD, "xpath", xpathConfig);

        // This should succeed as all parameters are valid
        XmlProcessor processor = createProcessor(config);
        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
    }
}
