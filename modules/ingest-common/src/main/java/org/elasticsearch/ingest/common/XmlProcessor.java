/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * Processor that parses XML documents and converts them to JSON objects using a single-pass streaming approach.
 *
 * Features:
 * - XML to JSON conversion with configurable structure options
 * - XPath extraction with namespace support
 * - Configurable options: force_array, force_content, remove_namespaces, to_lower
 * - Strict parsing mode for XML validation
 * - Empty value filtering with remove_empty_values option
 * - Logstash-compatible error handling and behavior
 */
public final class XmlProcessor extends AbstractProcessor {

    public static final String TYPE = "xml";

    private static final XPathFactory XPATH_FACTORY = XPathFactory.newInstance();

    // Pre-compiled pattern to detect namespace prefixes
    private static final Pattern NAMESPACE_PATTERN = Pattern.compile(".*\\b[a-zA-Z][a-zA-Z0-9_-]*:[a-zA-Z][a-zA-Z0-9_-]*.*");

    // Pre-configured SAX parser factories for secure XML parsing
    private static final javax.xml.parsers.SAXParserFactory SAX_PARSER_FACTORY = createSecureSaxParserFactory();
    private static final javax.xml.parsers.SAXParserFactory SAX_PARSER_FACTORY_NS = createSecureSaxParserFactoryNamespaceAware();
    private static final javax.xml.parsers.SAXParserFactory SAX_PARSER_FACTORY_STRICT = createSecureSaxParserFactoryStrict();
    private static final javax.xml.parsers.SAXParserFactory SAX_PARSER_FACTORY_NS_STRICT =
        createSecureSaxParserFactoryNamespaceAwareStrict();

    // Pre-configured document builder factory for DOM creation
    private static final DocumentBuilderFactory DOM_FACTORY = createSecureDocumentBuilderFactory();

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;
    private final boolean ignoreFailure;
    private final boolean toLower;
    private final boolean removeEmptyValues;
    private final boolean storeXml;
    private final boolean removeNamespaces;
    private final boolean forceContent;
    private final boolean forceArray;
    private final Map<String, String> xpathExpressions;
    private final Map<String, String> namespaces;
    private final Map<String, XPathExpression> compiledXPathExpressions;
    private final String parseOptions;

    XmlProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreMissing,
        boolean ignoreFailure,
        boolean toLower,
        boolean removeEmptyValues,
        boolean storeXml,
        boolean removeNamespaces,
        boolean forceContent,
        boolean forceArray,
        Map<String, String> xpathExpressions,
        Map<String, String> namespaces,
        String parseOptions
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.ignoreFailure = ignoreFailure;
        this.toLower = toLower;
        this.removeEmptyValues = removeEmptyValues;
        this.storeXml = storeXml;
        this.removeNamespaces = removeNamespaces;
        this.forceContent = forceContent;
        this.forceArray = forceArray;
        this.xpathExpressions = xpathExpressions != null ? Map.copyOf(xpathExpressions) : Map.of();
        this.namespaces = namespaces != null ? Map.copyOf(namespaces) : Map.of();
        this.compiledXPathExpressions = compileXPathExpressions(this.xpathExpressions, this.namespaces);
        this.parseOptions = parseOptions != null ? parseOptions : "";
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    public boolean isRemoveEmptyValues() {
        return removeEmptyValues;
    }

    public boolean isStoreXml() {
        return storeXml;
    }

    public boolean isRemoveNamespaces() {
        return removeNamespaces;
    }

    public boolean isForceContent() {
        return forceContent;
    }

    public boolean isStrict() {
        return "strict".equals(parseOptions);
    }

    public boolean isForceArray() {
        return forceArray;
    }

    public boolean hasNamespaces() {
        return namespaces.isEmpty() == false;
    }

    public Map<String, String> getNamespaces() {
        return namespaces;
    }

    public String getParseOptions() {
        return parseOptions;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object fieldValue = document.getFieldValue(field, Object.class, ignoreMissing);

        if (fieldValue == null) {
            if (ignoreMissing || ignoreFailure) {
                return document;
            }
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse XML");
        }

        if (fieldValue instanceof String == false) {
            if (ignoreFailure) {
                return document;
            }
            throw new IllegalArgumentException("field [" + field + "] is not a string, cannot parse XML");
        }

        String xmlString = (String) fieldValue;
        try {
            // Always use streaming parser for optimal performance and memory usage
            if (storeXml || xpathExpressions.isEmpty() == false) {
                parseXmlAndXPath(document, xmlString.trim());
            }
        } catch (Exception e) {
            if (ignoreFailure) {
                // Add failure tag similar to Logstash behavior
                document.appendFieldValue("tags", "_xmlparsefailure");
                return document;
            }
            throw new IllegalArgumentException("field [" + field + "] contains invalid XML: " + e.getMessage(), e);
        }

        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Determines if a value should be considered empty for filtering purposes.
     * Used by the remove_empty_values feature to filter out empty content.
     *
     * Considers empty:
     * - null values
     * - empty or whitespace-only strings
     * - empty Maps
     * - empty Lists
     *
     * @param value the value to check
     * @return true if the value should be considered empty
     */
    private boolean isEmptyValue(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof String) {
            return ((String) value).trim().isEmpty();
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).isEmpty();
        }
        if (value instanceof List) {
            return ((List<?>) value).isEmpty();
        }
        return false;
    }

    /**
     * Extract the text value from a DOM node for XPath result processing.
     * Handles different node types appropriately:
     * - TEXT_NODE and CDATA_SECTION_NODE: returns node value directly
     * - ATTRIBUTE_NODE: returns attribute value
     * - ELEMENT_NODE: returns text content (concatenated text of all descendants)
     * - Other node types: returns text content as fallback
     *
     * @param node the DOM node to extract text from
     * @return the text content of the node, or null if node is null
     */
    private String getNodeValue(Node node) {
        if (node == null) {
            return null;
        }

        switch (node.getNodeType()) {
            case Node.ATTRIBUTE_NODE:
            case Node.CDATA_SECTION_NODE:
            case Node.TEXT_NODE:
                return node.getNodeValue();
            case Node.ELEMENT_NODE:
            default:
                return node.getTextContent();
        }
    }

    /**
     * Applies force_array logic to ensure all fields are arrays when enabled.
     *
     * Behavior:
     * - If force_array is false: returns content unchanged
     * - If force_array is true and content is already a List: returns content unchanged
     * - If force_array is true and content is not a List: wraps content in a new ArrayList
     * - Handles null content appropriately (wraps null in array if force_array is true)
     *
     * @param elementName the name of the element (for context, not used in current implementation)
     * @param content the content to potentially wrap in an array
     * @return the content, optionally wrapped in an array based on force_array setting
     */
    private Object applyForceArray(String elementName, Object content) {
        if (forceArray && !(content instanceof List)) {
            List<Object> arrayContent = new ArrayList<>();
            arrayContent.add(content);  // Add content even if it's null (for empty elements)
            return arrayContent;
        }
        return content;
    }

    /**
     * Evaluates precompiled XPath expressions against a DOM document and adds results to the ingest document.
     *
     * Features:
     * - Uses precompiled XPath expressions for optimal performance
     * - Extracts text values from matched nodes (elements, attributes, text nodes)
     * - Single matches stored as strings, multiple matches as string arrays
     * - Respects ignoreFailure setting for XPath evaluation errors
     *
     * @param document the ingest document to add XPath results to
     * @param doc the DOM document to evaluate XPath expressions against
     * @throws Exception if XPath processing fails and ignoreFailure is false
     */
    private void processXPathExpressionsFromDom(IngestDocument document, Document doc) throws Exception {
        // Use precompiled XPath expressions for optimal performance
        for (Map.Entry<String, XPathExpression> entry : compiledXPathExpressions.entrySet()) {
            String targetFieldName = entry.getKey();
            XPathExpression compiledExpression = entry.getValue();

            try {
                Object result = compiledExpression.evaluate(doc, XPathConstants.NODESET);

                if (result instanceof NodeList) {
                    NodeList nodeList = (NodeList) result;
                    List<String> values = new ArrayList<>();

                    for (int i = 0; i < nodeList.getLength(); i++) {
                        Node node = nodeList.item(i);
                        String value = getNodeValue(node);
                        if (value != null && value.trim().isEmpty() == false) {
                            values.add(value);
                        }
                    }

                    if (values.isEmpty() == false) {
                        if (values.size() == 1) {
                            document.setFieldValue(targetFieldName, values.get(0));
                        } else {
                            document.setFieldValue(targetFieldName, values);
                        }
                    }
                }
            } catch (XPathExpressionException e) {
                if (ignoreFailure == false) {
                    throw new IllegalArgumentException(
                        "XPath evaluation failed for target field [" + targetFieldName + "]: " + e.getMessage(),
                        e
                    );
                }
            }
        }
    }

    /**
     * Compiles XPath expressions at processor creation time for optimal runtime performance.
     * This method pre-compiles all configured XPath expressions with appropriate namespace context,
     * eliminating the compilation overhead during document processing.
     *
     * @param xpathExpressions map of XPath expressions to target field names
     * @param namespaces map of namespace prefixes to URIs
     * @return map of compiled XPath expressions keyed by target field name
     * @throws IllegalArgumentException if XPath compilation fails or namespace validation fails
     */
    private static Map<String, XPathExpression> compileXPathExpressions(
        Map<String, String> xpathExpressions,
        Map<String, String> namespaces
    ) {
        if (xpathExpressions.isEmpty()) {
            return Map.of();
        }

        Map<String, XPathExpression> compiled = new HashMap<>();
        XPath xpath = XPATH_FACTORY.newXPath();

        // Set namespace context if namespaces are defined
        boolean hasNamespaces = namespaces.isEmpty() == false;
        if (hasNamespaces) {
            xpath.setNamespaceContext(new NamespaceContext() {
                @Override
                public String getNamespaceURI(String prefix) {
                    if (prefix == null) {
                        throw new IllegalArgumentException("Prefix cannot be null");
                    }
                    return namespaces.getOrDefault(prefix, "");
                }

                @Override
                public String getPrefix(String namespaceURI) {
                    for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                        if (entry.getValue().equals(namespaceURI)) {
                            return entry.getKey();
                        }
                    }
                    return null;
                }

                @Override
                public Iterator<String> getPrefixes(String namespaceURI) {
                    List<String> prefixes = new ArrayList<>();
                    for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                        if (entry.getValue().equals(namespaceURI)) {
                            prefixes.add(entry.getKey());
                        }
                    }
                    return prefixes.iterator();
                }
            });
        }

        // Use pre-compiled pattern to detect namespace prefixes

        for (Map.Entry<String, String> entry : xpathExpressions.entrySet()) {
            String xpathExpression = entry.getKey();
            String targetFieldName = entry.getValue();

            // Validate namespace prefixes if no namespaces are configured
            if (!hasNamespaces && NAMESPACE_PATTERN.matcher(xpathExpression).matches()) {
                throw new IllegalArgumentException(
                    "Invalid XPath expression ["
                        + xpathExpression
                        + "]: contains namespace prefixes but no namespace configuration provided"
                );
            }

            try {
                XPathExpression compiledExpression = xpath.compile(xpathExpression);
                compiled.put(targetFieldName, compiledExpression);
            } catch (XPathExpressionException e) {
                throw new IllegalArgumentException("Invalid XPath expression [" + xpathExpression + "]: " + e.getMessage(), e);
            }
        }

        return Map.copyOf(compiled);
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public XmlProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean ignoreFailure = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_failure", false);
            boolean toLower = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "to_lower", false);
            boolean removeEmptyValues = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_empty_values", false);
            boolean storeXml = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "store_xml", true);
            boolean removeNamespaces = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_namespaces", false);
            boolean forceContent = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "force_content", false);
            boolean forceArray = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "force_array", false);

            // Parse XPath expressions map
            Map<String, String> xpathExpressions = new HashMap<>();
            Object xpathConfig = config.get("xpath");
            if (xpathConfig != null) {
                if (xpathConfig instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> xpathMap = (Map<String, Object>) xpathConfig;
                    for (Map.Entry<String, Object> entry : xpathMap.entrySet()) {
                        if (entry.getValue() instanceof String) {
                            xpathExpressions.put(entry.getKey(), (String) entry.getValue());
                        } else {
                            throw new IllegalArgumentException(
                                "XPath target field ["
                                    + entry.getKey()
                                    + "] must be a string, got ["
                                    + entry.getValue().getClass().getSimpleName()
                                    + "]"
                            );
                        }
                    }
                } else {
                    throw new IllegalArgumentException("XPath configuration must be a map of expressions to target fields");
                }
            }

            // Parse namespaces map
            Map<String, String> namespaces = new HashMap<>();
            Object namespaceConfig = config.get("namespaces");
            if (namespaceConfig != null) {
                if (namespaceConfig instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> namespaceMap = (Map<String, Object>) namespaceConfig;
                    for (Map.Entry<String, Object> entry : namespaceMap.entrySet()) {
                        if (entry.getValue() instanceof String) {
                            namespaces.put(entry.getKey(), (String) entry.getValue());
                        } else {
                            throw new IllegalArgumentException(
                                "Namespace prefix ["
                                    + entry.getKey()
                                    + "] must have a string URI, got ["
                                    + entry.getValue().getClass().getSimpleName()
                                    + "]"
                            );
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Namespaces configuration must be a map of prefixes to URIs");
                }
            }

            // Parse parse_options parameter
            String parseOptions = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "parse_options", "");
            if (parseOptions != null && !parseOptions.isEmpty() && !"strict".equals(parseOptions)) {
                throw new IllegalArgumentException("Invalid parse_options [" + parseOptions + "]. Only 'strict' is supported.");
            }

            return new XmlProcessor(
                processorTag,
                description,
                field,
                targetField,
                ignoreMissing,
                ignoreFailure,
                toLower,
                removeEmptyValues,
                storeXml,
                removeNamespaces,
                forceContent,
                forceArray,
                xpathExpressions,
                namespaces,
                parseOptions
            );
        }
    }

    /**
     * Main XML parsing method that converts XML to JSON and optionally extracts XPath values.
     * Uses streaming SAX parser with optional DOM building for XPath processing.
     *
     * @param document the ingest document to modify with parsed results
     * @param xmlString the XML string to parse (should be trimmed)
     * @throws Exception if XML parsing fails
     */
    private void parseXmlAndXPath(IngestDocument document, String xmlString) throws Exception {
        if (xmlString == null || xmlString.trim().isEmpty()) {
            return;
        }

        // Determine if we need DOM for XPath processing
        boolean needsDom = xpathExpressions.isEmpty() == false;

        // Use the appropriate pre-configured SAX parser factory
        javax.xml.parsers.SAXParserFactory factory = selectSaxParserFactory();

        javax.xml.parsers.SAXParser parser = factory.newSAXParser();

        // Configure error handler for strict mode
        if (isStrict()) {
            parser.getXMLReader().setErrorHandler(new org.xml.sax.ErrorHandler() {
                @Override
                public void warning(org.xml.sax.SAXParseException exception) throws org.xml.sax.SAXException {
                    throw exception;
                }

                @Override
                public void error(org.xml.sax.SAXParseException exception) throws org.xml.sax.SAXException {
                    throw exception;
                }

                @Override
                public void fatalError(org.xml.sax.SAXParseException exception) throws org.xml.sax.SAXException {
                    throw exception;
                }
            });
        }

        // Use enhanced handler that can build DOM during streaming when needed
        XmlStreamingWithDomHandler handler = new XmlStreamingWithDomHandler(needsDom);

        parser.parse(new java.io.ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)), handler);

        // Store structured result if needed
        if (storeXml) {
            Object streamingResult = handler.getStructuredResult();
            if (streamingResult != null) {
                document.setFieldValue(targetField, streamingResult);
            }
        }

        // Process XPath expressions if DOM was built during streaming
        if (needsDom) {
            Document domDocument = handler.getDomDocument();
            if (domDocument != null) {
                processXPathExpressionsFromDom(document, domDocument);
            }
        }
    }

    /**
     * SAX ContentHandler that builds structured JSON output and optionally constructs a DOM tree during parsing.
     * Handles XML-to-JSON conversion with support for all processor configuration options.
     */
    private class XmlStreamingWithDomHandler extends org.xml.sax.helpers.DefaultHandler {
        // Streaming parser state (for structured output)
        private final java.util.Deque<Map<String, Object>> elementStack = new java.util.ArrayDeque<>();
        private final java.util.Deque<String> elementNameStack = new java.util.ArrayDeque<>();
        private final java.util.Deque<StringBuilder> textStack = new java.util.ArrayDeque<>();
        private final java.util.Deque<Map<String, List<Object>>> repeatedElementsStack = new java.util.ArrayDeque<>();
        private Object rootResult = null;

        // DOM building state (for XPath processing when needed)
        private final boolean buildDom;
        private Document domDocument = null;
        private final java.util.Deque<org.w3c.dom.Element> domElementStack = new java.util.ArrayDeque<>();

        public XmlStreamingWithDomHandler(boolean buildDom) {
            this.buildDom = buildDom;
        }

        @Override
        public void startDocument() throws org.xml.sax.SAXException {
            // Initialize DOM document if needed
            if (buildDom) {
                try {
                    // Use pre-configured secure DOM factory
                    // Since we build DOM programmatically (createElementNS/createElement),
                    // the factory's namespace awareness doesn't affect our usage
                    DocumentBuilder builder = DOM_FACTORY.newDocumentBuilder();
                    domDocument = builder.newDocument();
                } catch (Exception e) {
                    throw new org.xml.sax.SAXException("Failed to create DOM document", e);
                }
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, org.xml.sax.Attributes attributes)
            throws org.xml.sax.SAXException {
            String elementName = getElementName(uri, localName, qName);

            // Build structured representation (always)
            Map<String, Object> element = new HashMap<>();
            Map<String, List<Object>> repeatedElements = new HashMap<>();

            // Process attributes for structured output
            for (int i = 0; i < attributes.getLength(); i++) {
                String attrName = getAttributeName(attributes.getURI(i), attributes.getLocalName(i), attributes.getQName(i));
                String attrValue = attributes.getValue(i);

                // Apply removeEmptyValues filtering to attributes
                if (removeEmptyValues == false || isEmptyValue(attrValue) == false) {
                    element.put(attrName, attrValue);
                }
            }

            elementStack.push(element);
            elementNameStack.push(elementName);
            textStack.push(new StringBuilder());
            repeatedElementsStack.push(repeatedElements);

            // Build DOM element simultaneously if needed
            if (buildDom && domDocument != null) {
                org.w3c.dom.Element domElement;
                if (uri != null && !uri.isEmpty() && !removeNamespaces) {
                    domElement = domDocument.createElementNS(uri, qName);
                } else {
                    domElement = domDocument.createElement(removeNamespaces ? localName : qName);
                }

                // Add attributes to DOM element
                for (int i = 0; i < attributes.getLength(); i++) {
                    String attrUri = attributes.getURI(i);
                    String attrLocalName = attributes.getLocalName(i);
                    String attrQName = attributes.getQName(i);
                    String attrValue = attributes.getValue(i);

                    if (attrUri != null && !attrUri.isEmpty() && !removeNamespaces) {
                        domElement.setAttributeNS(attrUri, attrQName, attrValue);
                    } else {
                        domElement.setAttribute(removeNamespaces ? attrLocalName : attrQName, attrValue);
                    }
                }

                // Add to parent or root
                if (domElementStack.isEmpty()) {
                    domDocument.appendChild(domElement);
                } else {
                    domElementStack.peek().appendChild(domElement);
                }

                domElementStack.push(domElement);
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws org.xml.sax.SAXException {
            // Add to structured output text accumulator
            if (!textStack.isEmpty()) {
                textStack.peek().append(ch, start, length);
            }

            // Add to DOM text node if needed
            if (buildDom && !domElementStack.isEmpty()) {
                String text = new String(ch, start, length);
                if (!text.trim().isEmpty() || !removeEmptyValues) {
                    org.w3c.dom.Text textNode = domDocument.createTextNode(text);
                    domElementStack.peek().appendChild(textNode);
                }
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws org.xml.sax.SAXException {
            // Complete structured output element processing
            if (elementStack.isEmpty()) {
                return;
            }

            Map<String, Object> element = elementStack.pop();
            String elementName = elementNameStack.pop();
            StringBuilder textContent = textStack.pop();
            Map<String, List<Object>> repeatedElements = repeatedElementsStack.pop();

            // Add repeated elements as arrays
            for (Map.Entry<String, List<Object>> entry : repeatedElements.entrySet()) {
                List<Object> values = entry.getValue();
                if (removeEmptyValues == false || values.isEmpty() == false) {
                    element.put(entry.getKey(), values);
                }
            }

            // Process text content and determine final element structure
            String trimmedText = textContent.toString().trim();
            boolean hasText = trimmedText.isEmpty() == false;
            boolean hasChildren = element.size() > 0;

            Object elementValue;
            if (hasText == false && hasChildren == false) {
                // Empty element
                if (removeEmptyValues == false) {
                    elementValue = applyForceArray(elementName, null);
                } else {
                    elementValue = null;
                }
            } else if (hasText && hasChildren == false) {
                // Only text content
                if (forceContent) {
                    Map<String, Object> contentMap = new HashMap<>();
                    if (removeEmptyValues == false || isEmptyValue(trimmedText) == false) {
                        contentMap.put("#text", trimmedText);
                    }
                    elementValue = contentMap;
                } else {
                    if (removeEmptyValues && isEmptyValue(trimmedText)) {
                        elementValue = null;
                    } else {
                        elementValue = trimmedText;
                    }
                }
                elementValue = applyForceArray(elementName, elementValue);
            } else if (hasText == false && hasChildren) {
                // Only child elements/attributes
                elementValue = (forceArray && forceContent) ? applyForceArray(elementName, element) : element;
            } else {
                // Both text and children/attributes
                if (removeEmptyValues == false || isEmptyValue(trimmedText) == false) {
                    element.put("#text", trimmedText);
                }
                elementValue = (forceArray && forceContent) ? applyForceArray(elementName, element) : element;
            }

            // If this is the root element, store the result
            if (elementStack.isEmpty()) {
                if (elementValue != null) {
                    Map<String, Object> result = new HashMap<>();
                    result.put(elementName, elementValue);
                    rootResult = result;
                }
            } else {
                // Add to parent element
                if (elementValue != null) {
                    Map<String, Object> parentElement = elementStack.peek();
                    Map<String, List<Object>> parentRepeatedElements = repeatedElementsStack.peek();

                    if (parentElement.containsKey(elementName) || parentRepeatedElements.containsKey(elementName)) {
                        // Handle repeated elements
                        if (parentRepeatedElements.containsKey(elementName) == false) {
                            List<Object> list = new ArrayList<>();
                            list.add(parentElement.get(elementName));
                            parentRepeatedElements.put(elementName, list);
                            parentElement.remove(elementName);
                        }
                        parentRepeatedElements.get(elementName).add(elementValue);
                    } else {
                        // Apply force_array logic for single elements
                        Object finalContent = applyForceArray(elementName, elementValue);
                        parentElement.put(elementName, finalContent);
                    }
                }
            }

            // Complete DOM element if building DOM
            if (buildDom && !domElementStack.isEmpty()) {
                domElementStack.pop();
            }
        }

        @Override
        public void endDocument() throws org.xml.sax.SAXException {
            // Document parsing complete
        }

        public Object getStructuredResult() {
            return rootResult;
        }

        public Document getDomDocument() {
            return domDocument;
        }

        private String getElementName(String uri, String localName, String qName) {
            String elementName;
            if (removeNamespaces) {
                elementName = localName != null && !localName.isEmpty() ? localName : qName;
            } else {
                elementName = qName;
            }

            // Apply toLower if enabled
            if (toLower) {
                elementName = elementName.toLowerCase(Locale.ROOT);
            }

            return elementName;
        }

        private String getAttributeName(String uri, String localName, String qName) {
            String attrName;
            if (removeNamespaces) {
                attrName = localName != null && !localName.isEmpty() ? localName : qName;
            } else {
                attrName = qName;
            }

            // Apply toLower if enabled
            if (toLower) {
                attrName = attrName.toLowerCase(Locale.ROOT);
            }

            return attrName;
        }
    }

    /**
     * Creates a secure, pre-configured SAX parser factory for XML parsing.
     * This factory is configured to prevent XXE attacks with SAX-specific features.
     */
    private static javax.xml.parsers.SAXParserFactory createSecureSaxParserFactory() {
        javax.xml.parsers.SAXParserFactory factory = javax.xml.parsers.SAXParserFactory.newInstance();
        factory.setValidating(false);

        // Configure SAX-specific security features to prevent XXE attacks
        try {
            // SAX parser features - these are the correct features for SAXParserFactory
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        } catch (Exception e) {
            // If features cannot be set, continue with default settings
        }

        return factory;
    }

    /**
     * Creates a secure, pre-configured namespace-aware SAX parser factory for XML parsing.
     * This factory is configured to prevent XXE attacks and has namespace awareness enabled.
     */
    private static javax.xml.parsers.SAXParserFactory createSecureSaxParserFactoryNamespaceAware() {
        javax.xml.parsers.SAXParserFactory factory = javax.xml.parsers.SAXParserFactory.newInstance();
        factory.setValidating(false);
        factory.setNamespaceAware(true);

        // Configure SAX-specific security features to prevent XXE attacks
        try {
            // SAX parser features - these are the correct features for SAXParserFactory
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        } catch (Exception e) {
            // If features cannot be set, continue with default settings
        }

        return factory;
    }

    /**
     * Creates a secure, pre-configured SAX parser factory for strict XML parsing.
     * This factory is configured to prevent XXE attacks and has strict validation enabled.
     */
    private static javax.xml.parsers.SAXParserFactory createSecureSaxParserFactoryStrict() {
        javax.xml.parsers.SAXParserFactory factory = javax.xml.parsers.SAXParserFactory.newInstance();
        factory.setValidating(false);

        // Configure SAX-specific security features to prevent XXE attacks
        try {
            // SAX parser features - these are the correct features for SAXParserFactory
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

            // Enable strict parsing features
            factory.setFeature("http://apache.org/xml/features/validation/check-full-element-content", true);
        } catch (Exception e) {
            // If features cannot be set, continue with default settings
        }

        return factory;
    }

    /**
     * Creates a secure, pre-configured namespace-aware SAX parser factory for strict XML parsing.
     * This factory is configured to prevent XXE attacks, has namespace awareness enabled, and strict validation.
     */
    private static javax.xml.parsers.SAXParserFactory createSecureSaxParserFactoryNamespaceAwareStrict() {
        javax.xml.parsers.SAXParserFactory factory = javax.xml.parsers.SAXParserFactory.newInstance();
        factory.setValidating(false);
        factory.setNamespaceAware(true);

        // Configure SAX-specific security features to prevent XXE attacks
        try {
            // SAX parser features - these are the correct features for SAXParserFactory
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

            // Enable strict parsing features
            factory.setFeature("http://apache.org/xml/features/validation/check-full-element-content", true);
        } catch (Exception e) {
            // If features cannot be set, continue with default settings
        }

        return factory;
    }

    /**
     * Creates a secure, pre-configured DocumentBuilderFactory for DOM creation.
     * Since we only use this factory to create empty DOM documents programmatically
     * (not to parse XML), XXE security features are not needed here.
     * The SAX parser handles all XML parsing with appropriate security measures.
     */
    private static DocumentBuilderFactory createSecureDocumentBuilderFactory() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);  // Enable for maximum compatibility
        factory.setValidating(false);

        // No XXE security features needed - we only create empty documents,
        // never parse XML with this factory

        return factory;
    }

    /**
     * Selects the appropriate pre-configured SAX parser factory based on processor configuration.
     *
     * Factory selection matrix:
     * - Regular parsing, no namespaces: SAX_PARSER_FACTORY
     * - Regular parsing, with namespaces: SAX_PARSER_FACTORY_NS
     * - Strict parsing, no namespaces: SAX_PARSER_FACTORY_STRICT
     * - Strict parsing, with namespaces: SAX_PARSER_FACTORY_NS_STRICT
     *
     * @return the appropriate SAX parser factory for the current configuration
     */
    private javax.xml.parsers.SAXParserFactory selectSaxParserFactory() {
        boolean needsNamespaceAware = hasNamespaces() || removeNamespaces;
        if (isStrict()) {
            return needsNamespaceAware ? SAX_PARSER_FACTORY_NS_STRICT : SAX_PARSER_FACTORY_STRICT;
        } else {
            return needsNamespaceAware ? SAX_PARSER_FACTORY_NS : SAX_PARSER_FACTORY;
        }
    }
}
