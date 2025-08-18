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
     * Creates a basic configuration map with the specified field name.
     */
    private Map<String, Object> createBaseConfig(String fieldName) {
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        return config;
    }

    /**
     * Creates a basic configuration map with the default field name.
     */
    private Map<String, Object> createBaseConfig() {
        return createBaseConfig(DEFAULT_FIELD);
    }

    /**
     * Creates a configuration map with XPath expressions.
     */
    private Map<String, Object> createConfigWithXPath(String fieldName, Map<String, String> xpathExpressions) {
        Map<String, Object> config = createBaseConfig(fieldName);
        config.put("xpath", xpathExpressions);
        return config;
    }

    /**
     * Creates a configuration map with namespace definitions.
     */
    private Map<String, Object> createConfigWithNamespaces(String fieldName, Map<String, String> namespaces) {
        Map<String, Object> config = createBaseConfig(fieldName);
        config.put("namespaces", namespaces);
        return config;
    }

    /**
     * Creates a configuration map with both XPath expressions and namespaces.
     */
    private Map<String, Object> createConfigWithXPathAndNamespaces(
        String fieldName,
        Map<String, String> xpathExpressions,
        Map<String, String> namespaces
    ) {
        Map<String, Object> config = createBaseConfig(fieldName);
        config.put("xpath", xpathExpressions);
        config.put("namespaces", namespaces);
        return config;
    }

    /**
     * Creates a processor with the given factory and configuration.
     */
    private XmlProcessor createProcessor(XmlProcessor.Factory factory, Map<String, Object> config) throws Exception {
        String processorTag = randomAlphaOfLength(10);
        return factory.create(null, processorTag, null, config, null);
    }

    /**
     * Creates a processor with the default factory and given configuration.
     */
    private XmlProcessor createProcessor(Map<String, Object> config) throws Exception {
        return createProcessor(createFactory(), config);
    }

    /**
     * Helper method to create XPath configuration map.
     */
    private Map<String, String> createXPathConfig(String... expressionsAndFields) {
        if (expressionsAndFields.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide even number of arguments (expression, field, expression, field, ...)");
        }

        Map<String, String> xpathConfig = new HashMap<>();
        for (int i = 0; i < expressionsAndFields.length; i += 2) {
            xpathConfig.put(expressionsAndFields[i], expressionsAndFields[i + 1]);
        }
        return xpathConfig;
    }

    /**
     * Helper method to create namespace configuration map.
     */
    private Map<String, String> createNamespaceConfig(String... prefixesAndUris) {
        if (prefixesAndUris.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide even number of arguments (prefix, uri, prefix, uri, ...)");
        }

        Map<String, String> namespaceConfig = new HashMap<>();
        for (int i = 0; i < prefixesAndUris.length; i += 2) {
            namespaceConfig.put(prefixesAndUris[i], prefixesAndUris[i + 1]);
        }
        return namespaceConfig;
    }

    /**
     * Helper method to create configuration with common boolean options.
     */
    private Map<String, Object> createConfigWithOptions(String fieldName, String... options) {
        Map<String, Object> config = createBaseConfig(fieldName);

        for (String option : options) {
            switch (option) {
                case "ignore_missing":
                    config.put("ignore_missing", true);
                    break;
                case "ignore_failure":
                    config.put("ignore_failure", true);
                    break;
                case "to_lower":
                    config.put("to_lower", true);
                    break;
                case "remove_empty_values":
                    config.put("remove_empty_values", true);
                    break;
                case "store_xml":
                    config.put("store_xml", false); // Test false case since default is true
                    break;
                case "remove_namespaces":
                    config.put("remove_namespaces", true);
                    break;
                case "force_content":
                    config.put("force_content", true);
                    break;
                case "force_array":
                    config.put("force_array", true);
                    break;
                case "strict":
                    config.put("parse_options", "strict");
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option: " + option);
            }
        }

        return config;
    }

    /**
     * Helper to expect processor creation failure with specific message.
     */
    private void expectCreationFailure(Map<String, Object> config, Class<? extends Exception> exceptionClass, String expectedMessage) {
        XmlProcessor.Factory factory = createFactory();
        String processorTag = randomAlphaOfLength(10);

        Exception exception = expectThrows(exceptionClass, () -> factory.create(null, processorTag, null, config, null));
        assertThat(exception.getMessage(), equalTo(expectedMessage));
    }

    /**
     * Tests processor creation with various configurations.
     */
    public void testCreate() throws Exception {
        Map<String, Object> config = createBaseConfig();
        config.put("target_field", DEFAULT_TARGET_FIELD);
        config.put("ignore_missing", true);
        config.put("ignore_failure", true);
        config.put("to_lower", true);
        config.put("remove_empty_values", true);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getTargetField(), equalTo(DEFAULT_TARGET_FIELD));
        assertThat(processor.isIgnoreMissing(), equalTo(true));
        assertThat(processor.isRemoveEmptyValues(), equalTo(true));
    }

    public void testCreateWithDefaults() throws Exception {
        Map<String, Object> config = createBaseConfig();
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getTargetField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isIgnoreMissing(), equalTo(false));
        assertThat(processor.isRemoveEmptyValues(), equalTo(false));
    }

    public void testCreateMissingField() throws Exception {
        Map<String, Object> config = new HashMap<>(); // Empty config - no field specified
        expectCreationFailure(config, ElasticsearchParseException.class, "[field] required property is missing");
    }

    public void testCreateWithRemoveEmptyValuesOnly() throws Exception {
        Map<String, Object> config = createBaseConfig();
        config.put("remove_empty_values", true);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isRemoveEmptyValues(), equalTo(true));
        assertThat(processor.isIgnoreMissing(), equalTo(false)); // other flags should remain default
    }

    public void testCreateWithXPath() throws Exception {
        Map<String, String> xpathConfig = createXPathConfig("//author/text()", "author_field", "//title/@lang", "language_field");
        Map<String, Object> config = createConfigWithXPath(DEFAULT_FIELD, xpathConfig);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
    }

    public void testCreateWithInvalidXPathConfig() throws Exception {
        Map<String, Object> config = createBaseConfig();
        config.put("xpath", "invalid_string"); // Should be a map

        expectCreationFailure(config, IllegalArgumentException.class, "XPath configuration must be a map of expressions to target fields");
    }

    public void testCreateWithInvalidXPathTargetField() throws Exception {
        Map<String, Object> config = createBaseConfig();

        Map<String, Object> xpathConfig = new HashMap<>();
        xpathConfig.put("//author/text()", 123); // Should be string
        config.put("xpath", xpathConfig);

        expectCreationFailure(
            config,
            IllegalArgumentException.class,
            "XPath target field [//author/text()] must be a string, got [Integer]"
        );
    }

    public void testCreateWithNamespaces() throws Exception {
        Map<String, String> namespacesConfig = createNamespaceConfig(
            "book",
            "http://example.com/book",
            "author",
            "http://example.com/author"
        );
        Map<String, Object> config = createConfigWithNamespaces(DEFAULT_FIELD, namespacesConfig);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getNamespaces(), equalTo(namespacesConfig));
    }

    public void testCreateWithInvalidNamespacesConfig() throws Exception {
        Map<String, Object> config = createBaseConfig();
        config.put("namespaces", "invalid_string"); // Should be a map

        expectCreationFailure(config, IllegalArgumentException.class, "Namespaces configuration must be a map of prefixes to URIs");
    }

    public void testCreateWithInvalidNamespaceURI() throws Exception {
        Map<String, Object> config = createBaseConfig();

        Map<String, Object> namespacesConfig = new HashMap<>();
        namespacesConfig.put("book", 123); // Should be string
        config.put("namespaces", namespacesConfig);

        expectCreationFailure(config, IllegalArgumentException.class, "Namespace prefix [book] must have a string URI, got [Integer]");
    }

    public void testCreateWithXPathAndNamespaces() throws Exception {
        Map<String, String> xpathConfig = createXPathConfig("//book:author/text()", "author_field", "//book:title/@lang", "language_field");
        Map<String, String> namespacesConfig = createNamespaceConfig("book", "http://example.com/book");
        Map<String, Object> config = createConfigWithXPathAndNamespaces(DEFAULT_FIELD, xpathConfig, namespacesConfig);

        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getNamespaces(), equalTo(namespacesConfig));
    }

    // Tests for individual boolean options

    public void testCreateWithStoreXmlFalse() throws Exception {
        Map<String, Object> config = createConfigWithOptions(DEFAULT_FIELD, "store_xml");
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isStoreXml(), equalTo(false));
    }

    public void testCreateWithRemoveNamespaces() throws Exception {
        Map<String, Object> config = createConfigWithOptions(DEFAULT_FIELD, "remove_namespaces");
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isRemoveNamespaces(), equalTo(true));
    }

    public void testCreateWithForceContent() throws Exception {
        Map<String, Object> config = createConfigWithOptions(DEFAULT_FIELD, "force_content");
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isForceContent(), equalTo(true));
    }

    public void testCreateWithForceArray() throws Exception {
        Map<String, Object> config = createConfigWithOptions(DEFAULT_FIELD, "force_array");
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isForceArray(), equalTo(true));
    }

    public void testCreateWithStrictParseOptions() throws Exception {
        Map<String, Object> config = createConfigWithOptions(DEFAULT_FIELD, "strict");
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.getParseOptions(), equalTo("strict"));
        assertThat(processor.isStrict(), equalTo(true));
    }

    public void testCreateWithMultipleOptions() throws Exception {
        Map<String, Object> config = createConfigWithOptions(
            DEFAULT_FIELD,
            "ignore_missing",
            "force_content",
            "force_array",
            "remove_namespaces"
        );
        XmlProcessor processor = createProcessor(config);

        assertThat(processor.getField(), equalTo(DEFAULT_FIELD));
        assertThat(processor.isIgnoreMissing(), equalTo(true));
        assertThat(processor.isForceContent(), equalTo(true));
        assertThat(processor.isForceArray(), equalTo(true));
        assertThat(processor.isRemoveNamespaces(), equalTo(true));
    }

    // Tests for invalid parse options

    public void testCreateWithInvalidParseOptions() throws Exception {
        Map<String, Object> config = createBaseConfig();
        config.put("parse_options", "invalid_option");

        expectCreationFailure(
            config,
            IllegalArgumentException.class,
            "Invalid parse_options [invalid_option]. Only 'strict' is supported."
        );
    }

    // Tests for XPath compilation errors (testing precompilation feature)

    public void testCreateWithInvalidXPathExpression() throws Exception {
        Map<String, String> xpathConfig = createXPathConfig("invalid xpath ][", "target_field");
        Map<String, Object> config = createConfigWithXPath(DEFAULT_FIELD, xpathConfig);

        XmlProcessor.Factory factory = createFactory();
        String processorTag = randomAlphaOfLength(10);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );

        // Check that the error message contains the XPath expression and indicates it's invalid
        assertThat(exception.getMessage(), containsString("Invalid XPath expression [invalid xpath ][]:"));
        assertThat(exception.getMessage(), containsString("javax.xml.transform.TransformerException"));
    }

    public void testCreateWithXPathUsingNamespacesWithoutConfiguration() throws Exception {
        Map<String, String> xpathConfig = createXPathConfig("//book:title/text()", "title_field");
        Map<String, Object> config = createConfigWithXPath(DEFAULT_FIELD, xpathConfig);

        expectCreationFailure(
            config,
            IllegalArgumentException.class,
            "Invalid XPath expression [//book:title/text()]: contains namespace prefixes but no namespace configuration provided"
        );
    }
}
