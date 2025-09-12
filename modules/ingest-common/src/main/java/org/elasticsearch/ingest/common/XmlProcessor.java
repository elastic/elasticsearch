/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.XmlUtils;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.lang.ref.SoftReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * Processor that parses XML documents and converts them to JSON objects using a single-pass streaming approach.
 *
 * Features:<ul>
 *  <li>XML to JSON conversion with configurable structure options
 *  <li>XPath extraction with namespace support
 *  <li>Configurable options: force_array, force_content, remove_namespaces, to_lower
 *  <li>Empty value filtering with remove_empty_values option
 *  <li>Logstash-compatible error handling and behavior
 * </ul>
 */
public final class XmlProcessor extends AbstractProcessor {

    public static final String TYPE = "xml";
    private static final Logger logger = LogManager.getLogger(XmlProcessor.class);

    private static final XPathFactory XPATH_FACTORY = XPathFactory.newInstance();

    // Pre-compiled pattern to detect namespace prefixes
    private static final Pattern NAMESPACE_PATTERN = Pattern.compile("\\b[a-zA-Z][a-zA-Z0-9_-]*:[a-zA-Z][a-zA-Z0-9_-]*");

    /**
     * Lazily-initialized XML factories to avoid node startup failures if the JDK doesn't support required functionality.
     * This inner class will only be loaded when XML processing is actually used.
     */
    private static class XmlFactories {
        // Pre-configured secure XML parser factories using XmlUtils
        static final SAXParserFactory SAX_PARSER_FACTORY = createSecureSaxParserFactory();
        static final SAXParserFactory SAX_PARSER_FACTORY_NS = createSecureSaxParserFactoryNamespaceAware();

        // Pre-configured secure document builder factory for DOM creation
        static final DocumentBuilderFactory DOM_FACTORY = createSecureDocumentBuilderFactory();
    }

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;
    private final boolean toLower;
    private final boolean removeEmptyValues;
    private final boolean storeXml;
    private final boolean removeNamespaces;
    private final boolean forceContent;
    private final boolean forceArray;
    private final Map<String, String> xpathExpressions;
    private final Map<String, String> namespaces;

    private final Map<String, XPathExpression> compiledXPathExpressions;
    private final boolean needsDom;
    private final SAXParserFactory factory;

    XmlProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreMissing,
        boolean toLower,
        boolean removeEmptyValues,
        boolean storeXml,
        boolean removeNamespaces,
        boolean forceContent,
        boolean forceArray,
        Map<String, String> xpathExpressions,
        Map<String, String> namespaces
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.toLower = toLower;
        this.removeEmptyValues = removeEmptyValues;
        this.storeXml = storeXml;
        this.removeNamespaces = removeNamespaces;
        this.forceContent = forceContent;
        this.forceArray = forceArray;

        this.xpathExpressions = xpathExpressions != null ? Map.copyOf(xpathExpressions) : Map.of();
        this.namespaces = namespaces != null ? Map.copyOf(namespaces) : Map.of();

        this.compiledXPathExpressions = compileXPathExpressions(this.xpathExpressions, this.namespaces);
        this.needsDom = this.xpathExpressions.isEmpty() == false;
        this.factory = selectSaxParserFactory(this.namespaces.isEmpty() == false || removeNamespaces);
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

    public boolean isForceArray() {
        return forceArray;
    }

    public Map<String, String> getNamespaces() {
        return namespaces;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object fieldValue = document.getFieldValue(field, Object.class, ignoreMissing);

        if (fieldValue == null) {
            if (ignoreMissing) {
                return document;
            }
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse XML");
        }

        if (fieldValue instanceof String == false) {
            throw new IllegalArgumentException("field [" + field + "] is not a string, cannot parse XML");
        }

        String xmlString = (String) fieldValue;
        try {
            // Always use streaming parser for optimal performance and memory usage
            if (storeXml || xpathExpressions.isEmpty() == false) {
                parseXmlAndXPath(document, xmlString.trim());
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("field [" + field + "] contains invalid XML", e);
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
     * Considers empty:<ul>
     *  <li>null values
     *  <li>empty or whitespace-only strings
     *  <li>empty Maps
     *  <li>empty Lists
     * </ul>
     *
     * @param value the value to check
     * @return true if the value should be considered empty
     */
    private boolean isEmptyValue(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof String string) {
            return string.isBlank();
        }
        if (value instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        if (value instanceof List<?> list) {
            return list.isEmpty();
        }
        return false;
    }

    /**
     * Extract the text value from a DOM node for XPath result processing.
     * Handles different node types appropriately:<ul>
     *  <li>TEXT_NODE and CDATA_SECTION_NODE: returns node value directly
     *  <li>ATTRIBUTE_NODE: returns attribute value
     *  <li>ELEMENT_NODE: returns text content (concatenated text of all descendants)
     *  <li>Other node types: returns text content as fallback
     * </ul>
     *
     * @param node the DOM node to extract text from
     * @return the text content of the node, or null if node is null
     */
    private String getNodeValue(Node node) {
        if (node == null) {
            return null;
        }

        return switch (node.getNodeType()) {
            case Node.ATTRIBUTE_NODE, Node.CDATA_SECTION_NODE, Node.TEXT_NODE -> node.getNodeValue();
            default -> node.getTextContent();
        };
    }

    /**
     * Applies force_array logic to ensure all fields are arrays when enabled.
     *
     * Behavior:<ul>
     *  <li>If force_array is false: returns content unchanged
     *  <li>If force_array is true and content is already a List: returns content unchanged
     *  <li>If force_array is true and content is not a List: wraps content in a new ArrayList
     *  <li>Handles null content appropriately (wraps null in array if force_array is true)
     * </ul>
     *
     * @param elementName the name of the element (for context, not used in current implementation)
     * @param content the content to potentially wrap in an array
     * @return the content, optionally wrapped in an array based on force_array setting
     */
    private Object applyForceArray(String elementName, Object content) {
        if (forceArray && (content instanceof List) == false) {
            List<Object> arrayContent = new ArrayList<>();
            arrayContent.add(content);  // Add content even if it's null (for empty elements)
            return arrayContent;
        }
        return content;
    }

    /**
     * Evaluates precompiled XPath expressions against a DOM document and adds results to the ingest document.
     *
     * Features:<ul>
     *  <li>Uses precompiled XPath expressions for optimal performance
     *  <li>Extracts text values from matched nodes (elements, attributes, text nodes)
     *  <li>Single matches stored as strings, multiple matches as string arrays
     * </ul>
     *
     * @param ingestDocument the ingest document to add XPath results to
     * @param xmlDocument the DOM document to evaluate XPath expressions against
     * @throws XPathExpressionException if XPath processing fails
     */
    private void processXPathExpressionsFromDom(IngestDocument ingestDocument, Document xmlDocument) throws XPathExpressionException {
        // Use precompiled XPath expressions for optimal performance
        for (Map.Entry<String, XPathExpression> entry : compiledXPathExpressions.entrySet()) {
            String targetFieldName = entry.getKey();
            XPathExpression compiledExpression = entry.getValue();
            Object result = compiledExpression.evaluate(xmlDocument, XPathConstants.NODESET);

            if (result instanceof NodeList nodeList) {
                // separate the case for 1 vs multiple nodeList elements to avoid unnecessary array allocation, this optimization is only
                // done because this is a per-document hot code path
                if (nodeList.getLength() == 1) {
                    Node node = nodeList.item(0);
                    String value = getNodeValue(node);
                    if (Strings.hasText(value)) {
                        ingestDocument.setFieldValue(targetFieldName, value);
                    }
                } else if (nodeList.getLength() > 1) {
                    List<String> values = new ArrayList<>();
                    for (int i = 0; i < nodeList.getLength(); i++) {
                        Node node = nodeList.item(i);
                        String value = getNodeValue(node);
                        if (Strings.hasText(value)) {
                            values.add(value);
                        }
                    }

                    if (values.isEmpty() == false) {
                        if (values.size() == 1) {
                            ingestDocument.setFieldValue(targetFieldName, values.get(0));
                        } else {
                            ingestDocument.setFieldValue(targetFieldName, values);
                        }
                    }
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
            // build a read-only reverse map for quick look up
            Map<String, Set<String>> uriToPrefixes = new HashMap<>();
            for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                uriToPrefixes.computeIfAbsent(entry.getValue(), k -> new HashSet<>()).add(entry.getKey());
            }
            uriToPrefixes.replaceAll((k, v) -> Collections.unmodifiableSet(v));

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
                    if (namespaceURI == null) {
                        throw new IllegalArgumentException("namespaceURI cannot be null");
                    }
                    if (uriToPrefixes.containsKey(namespaceURI)) {
                        return uriToPrefixes.get(namespaceURI).iterator().next();
                    } else {
                        return null;
                    }
                }

                @Override
                public Iterator<String> getPrefixes(String namespaceURI) {
                    if (namespaceURI == null) {
                        throw new IllegalArgumentException("namespaceURI cannot be null");
                    }
                    return uriToPrefixes.getOrDefault(namespaceURI, Set.of()).iterator();
                }
            });
        }

        // Use pre-compiled pattern to detect namespace prefixes

        for (Map.Entry<String, String> entry : xpathExpressions.entrySet()) {
            String xpathExpression = entry.getKey();
            String targetFieldName = entry.getValue();

            // Validate namespace prefixes if no namespaces are configured
            if (hasNamespaces == false && NAMESPACE_PATTERN.matcher(xpathExpression).find()) {
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
                throw new IllegalArgumentException("Invalid XPath expression [" + xpathExpression + "]", e);
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
            boolean toLower = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "to_lower", false);
            boolean removeEmptyValues = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_empty_values", false);
            boolean storeXml = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "store_xml", true);
            boolean removeNamespaces = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_namespaces", false);
            boolean forceContent = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "force_content", false);
            boolean forceArray = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "force_array", false);

            // Parse XPath expressions map
            Map<String, String> xpathExpressions = new HashMap<>();
            Map<String, Object> xpathConfig = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, "xpath");
            if (xpathConfig != null) {
                for (Map.Entry<String, Object> entry : xpathConfig.entrySet()) {
                    if (entry.getValue() instanceof String str) {
                        xpathExpressions.put(entry.getKey(), str);
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
            }

            // Parse namespaces map
            Map<String, String> namespaces = new HashMap<>();
            Map<String, Object> namespaceConfig = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, "namespaces");
            if (namespaceConfig != null) {
                for (Map.Entry<String, Object> entry : namespaceConfig.entrySet()) {
                    if (entry.getValue() instanceof String str) {
                        namespaces.put(entry.getKey(), str);
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
            }

            return new XmlProcessor(
                processorTag,
                description,
                field,
                targetField,
                ignoreMissing,
                toLower,
                removeEmptyValues,
                storeXml,
                removeNamespaces,
                forceContent,
                forceArray,
                xpathExpressions,
                namespaces
            );
        }
    }

    private static final Map<SAXParserFactory, ThreadLocal<SoftReference<SAXParser>>> PARSERS = new ConcurrentHashMap<>();
    static {
        PARSERS.put(XmlFactories.SAX_PARSER_FACTORY, new ThreadLocal<>());
        PARSERS.put(XmlFactories.SAX_PARSER_FACTORY_NS, new ThreadLocal<>());
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
        if (Strings.hasText(xmlString) == false) {
            return;
        }

        final SAXParser parser = getParser(factory);
        final XmlStreamingWithDomHandler handler;
        try {
            // Use enhanced handler that can build DOM during streaming when needed
            handler = new XmlStreamingWithDomHandler(needsDom);
            parser.parse(new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)), handler);
        } finally {
            parser.reset();
        }

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
            assert domDocument != null : "DOM document should not be null when XPath processing is needed";
            processXPathExpressionsFromDom(document, domDocument);
        }
    }

    /**
     * Gets the parser reference or creates a new one if the soft reference has been cleared.
     */
    private SAXParser getParser(SAXParserFactory factory) throws SAXException, ParserConfigurationException {
        final ThreadLocal<SoftReference<SAXParser>> threadLocal = PARSERS.getOrDefault(factory, new ThreadLocal<>());
        final SoftReference<SAXParser> parserReference = threadLocal.get();

        SAXParser parser = parserReference != null ? parserReference.get() : null;
        if (parser == null) {
            parser = factory.newSAXParser();
            threadLocal.set(new SoftReference<>(parser));
        }
        return parser;
    }

    /**
     * SAX ContentHandler that builds structured JSON output and optionally constructs a DOM tree during parsing.
     * Handles XML-to-JSON conversion with support for all processor configuration options.
     */
    private class XmlStreamingWithDomHandler extends org.xml.sax.helpers.DefaultHandler {

        /**
         * Record to encapsulate the parsing state for each XML element level.
         * Maintains the 1:1:1:1 relationship between element data, name, text content, and repeated elements.
         */
        private record ElementParsingState(
            Map<String, Object> element,
            String elementName,
            StringBuilder textContent,
            Map<String, List<Object>> repeatedElements
        ) {}

        // Streaming parser state (for structured output)
        private final java.util.Deque<ElementParsingState> elementStack = new java.util.ArrayDeque<>();
        private Object rootResult = null;

        // DOM building state (for XPath processing when needed)
        private final boolean buildDom;
        private Document domDocument = null;
        private final java.util.Deque<org.w3c.dom.Element> domElementStack = new java.util.ArrayDeque<>();

        XmlStreamingWithDomHandler(boolean buildDom) {
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
                    DocumentBuilder builder = XmlFactories.DOM_FACTORY.newDocumentBuilder();
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

            elementStack.push(new ElementParsingState(element, elementName, new StringBuilder(), repeatedElements));

            // Build DOM element simultaneously if needed
            if (buildDom && domDocument != null) {
                org.w3c.dom.Element domElement;
                if (uri != null && uri.isEmpty() == false && removeNamespaces == false) {
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

                    if (attrUri != null && attrUri.isEmpty() == false && removeNamespaces == false) {
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
            if (elementStack.isEmpty() == false) {
                elementStack.peek().textContent().append(ch, start, length);
            }

            // Add to DOM text node if needed
            if (buildDom && domElementStack.isEmpty() == false) {
                String text = new String(ch, start, length);
                if (text.isBlank() == false || removeEmptyValues == false) {
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

            ElementParsingState currentState = elementStack.pop();
            Map<String, Object> element = currentState.element();
            String elementName = currentState.elementName();
            StringBuilder textContent = currentState.textContent();
            Map<String, List<Object>> repeatedElements = currentState.repeatedElements();

            // Add repeated elements as arrays
            for (Map.Entry<String, List<Object>> entry : repeatedElements.entrySet()) {
                List<Object> values = entry.getValue();
                if (removeEmptyValues == false || values.isEmpty() == false) {
                    element.put(entry.getKey(), values);
                }
            }

            // Process text content and determine final element structure
            String textContentString = textContent.toString();
            String trimmedText = textContentString.trim();
            boolean hasText = textContentString.isBlank() == false;
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
                    ElementParsingState parentState = elementStack.peek();
                    Map<String, Object> parentElement = parentState.element();
                    Map<String, List<Object>> parentRepeatedElements = parentState.repeatedElements();

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
            if (buildDom && domElementStack.isEmpty() == false) {
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
                elementName = localName != null && localName.isEmpty() == false ? localName : qName;
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
                attrName = localName != null && localName.isEmpty() == false ? localName : qName;
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
     * Creates a secure, pre-configured SAX parser factory for XML parsing using XmlUtils.
     */
    private static SAXParserFactory createSecureSaxParserFactory() {
        try {
            SAXParserFactory factory = XmlUtils.getHardenedSaxParserFactory();
            factory.setValidating(false);
            factory.setNamespaceAware(false);
            return factory;
        } catch (Exception e) {
            logger.warn("Cannot configure secure XML parsing features - XML processor may not work correctly", e);
            return null;
        }
    }

    /**
     * Creates a secure, pre-configured namespace-aware SAX parser factory for XML parsing using XmlUtils.
     */
    private static SAXParserFactory createSecureSaxParserFactoryNamespaceAware() {
        try {
            SAXParserFactory factory = XmlUtils.getHardenedSaxParserFactory();
            factory.setValidating(false);
            factory.setNamespaceAware(true);
            return factory;
        } catch (Exception e) {
            logger.warn("Cannot configure secure namespace-aware XML parsing features - XML processor may not work correctly", e);
            return null;
        }
    }

    /**
     * Creates a secure, pre-configured DocumentBuilderFactory for DOM creation using XmlUtils.
     * Since we only use this factory to create empty DOM documents programmatically
     * (not to parse XML), we use the hardened builder factory.
     * The SAX parser handles all XML parsing with appropriate security measures.
     */
    private static DocumentBuilderFactory createSecureDocumentBuilderFactory() {
        try {
            DocumentBuilderFactory factory = XmlUtils.getHardenedBuilderFactory();
            factory.setValidating(false);  // Override validation for DOM creation
            return factory;
        } catch (Exception e) {
            logger.warn("Cannot configure secure DOM builder factory - XML processor may not work correctly", e);
            return null;
        }
    }

    /**
     * Selects the appropriate pre-configured SAX parser factory based on processor configuration.
     *
     * Factory selection matrix:<ul>
     *  <li>Regular parsing, no namespaces: SAX_PARSER_FACTORY
     *  <li>Regular parsing, with namespaces: SAX_PARSER_FACTORY_NS
     * </ul>
     *
     * @return the appropriate SAX parser factory for the current configuration
     * @throws UnsupportedOperationException if the required XML factory is not available
     */
    private static SAXParserFactory selectSaxParserFactory(final boolean needsNamespaceAware) {
        SAXParserFactory factory = needsNamespaceAware ? XmlFactories.SAX_PARSER_FACTORY_NS : XmlFactories.SAX_PARSER_FACTORY;
        if (factory == null) {
            throw new UnsupportedOperationException(
                "XML parsing"
                    + (needsNamespaceAware ? " with namespace-aware features " : " ")
                    + "is not supported by the current JDK. Please update your JDK to one that "
                    + "supports these XML features."
            );
        }
        return factory;
    }
}
