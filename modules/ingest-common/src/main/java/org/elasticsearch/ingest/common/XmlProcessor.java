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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Processor that parses XML documents and converts them to JSON objects using streaming XML parser.
 * This implementation uses XMLStreamReader for efficient parsing and avoids loading the entire document in memory.
 */
public final class XmlProcessor extends AbstractProcessor {

    public static final String TYPE = "xml";

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;
    private final boolean ignoreFailure;
    private final boolean toLower;
    private final boolean ignoreEmptyValue;

    XmlProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreMissing,
        boolean ignoreFailure,
        boolean toLower,
        boolean ignoreEmptyValue
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.ignoreFailure = ignoreFailure;
        this.toLower = toLower;
        this.ignoreEmptyValue = ignoreEmptyValue;
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

    public boolean isIgnoreEmptyValue() {
        return ignoreEmptyValue;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object fieldValue = document.getFieldValue(field, Object.class, ignoreMissing);

        if (fieldValue == null && ignoreMissing) {
            return document;
        } else if (fieldValue == null) {
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
            Object parsedXml = parseXml(xmlString.trim());
            if (ignoreEmptyValue) {
                parsedXml = filterEmptyValues(parsedXml);
            }
            document.setFieldValue(targetField, parsedXml);
        } catch (Exception e) {
            if (ignoreFailure) {
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
     * Recursively removes null and empty values from the parsed XML structure
     * when ignoreEmptyValue is enabled.
     */
    @SuppressWarnings("unchecked")
    private Object filterEmptyValues(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) obj;
            Map<String, Object> filtered = new HashMap<>();

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                Object filteredValue = filterEmptyValues(entry.getValue());
                if (filteredValue != null && isEmptyValue(filteredValue) == false) {
                    filtered.put(entry.getKey(), filteredValue);
                }
            }

            return filtered.isEmpty() ? null : filtered;
        }

        if (obj instanceof List) {
            List<Object> list = (List<Object>) obj;
            List<Object> filtered = new ArrayList<>();

            for (Object item : list) {
                Object filteredItem = filterEmptyValues(item);
                if (filteredItem != null && isEmptyValue(filteredItem) == false) {
                    filtered.add(filteredItem);
                }
            }

            return filtered.isEmpty() ? null : filtered;
        }

        return isEmptyValue(obj) ? null : obj;
    }

    /**
     * Determines if a value should be considered empty for filtering purposes.
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

    private Object parseXml(String xmlString) throws XMLStreamException {
        if (xmlString == null || xmlString.trim().isEmpty()) {
            return null;
        }

        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

        try (StringReader reader = new StringReader(xmlString)) {
            XMLStreamReader xmlReader = factory.createXMLStreamReader(reader);

            // Skip to the first element
            while (xmlReader.hasNext() && xmlReader.getEventType() != XMLStreamConstants.START_ELEMENT) {
                xmlReader.next();
            }

            if (xmlReader.hasNext() == false) {
                return null;
            }

            Object result = parseElement(xmlReader);
            xmlReader.close();
            return result;
        }
    }

    private Object parseElement(XMLStreamReader reader) throws XMLStreamException {
        if (reader.getEventType() != XMLStreamConstants.START_ELEMENT) {
            return null;
        }

        String elementName = reader.getLocalName();
        if (toLower) {
            elementName = elementName.toLowerCase(Locale.ROOT);
        }

        Map<String, Object> element = new HashMap<>();
        Map<String, List<Object>> repeatedElements = new HashMap<>();

        // Parse attributes - they are available in START_ELEMENT state
        int attributeCount = reader.getAttributeCount();
        boolean hasAttributes = attributeCount > 0;
        for (int i = 0; i < attributeCount; i++) {
            String attrName = reader.getAttributeLocalName(i);
            String attrValue = reader.getAttributeValue(i);
            if (toLower) {
                attrName = attrName.toLowerCase(Locale.ROOT);
            }
            element.put(attrName, attrValue);
        }

        StringBuilder textContent = new StringBuilder();

        while (reader.hasNext()) {
            int eventType = reader.next();

            switch (eventType) {
                case XMLStreamConstants.START_ELEMENT:
                    Object childElementResult = parseElement(reader);
                    String childName = reader.getLocalName();
                    if (toLower) {
                        childName = childName.toLowerCase(Locale.ROOT);
                    }

                    // Extract the actual content from the child element result
                    Object childContent = null;
                    if (childElementResult instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> childMap = (Map<String, Object>) childElementResult;
                        // The child element returns {elementName: content}, we want just the content
                        childContent = childMap.get(childName);
                    } else {
                        childContent = childElementResult;
                    }

                    if (element.containsKey(childName) || repeatedElements.containsKey(childName)) {
                        // Handle repeated elements
                        if (repeatedElements.containsKey(childName) == false) {
                            List<Object> list = new ArrayList<>();
                            list.add(element.get(childName));
                            repeatedElements.put(childName, list);
                            element.remove(childName);
                        }
                        repeatedElements.get(childName).add(childContent);
                    } else {
                        element.put(childName, childContent);
                    }
                    break;

                case XMLStreamConstants.CHARACTERS:
                    String text = reader.getText();
                    if (text != null && text.trim().isEmpty() == false) {
                        textContent.append(text);
                    }
                    break;

                case XMLStreamConstants.END_ELEMENT:
                    // Add repeated elements as arrays
                    for (Map.Entry<String, List<Object>> entry : repeatedElements.entrySet()) {
                        element.put(entry.getKey(), entry.getValue());
                    }

                    // Determine what to return
                    String trimmedText = textContent.toString().trim();
                    boolean hasText = trimmedText.isEmpty() == false;
                    boolean hasChildren = element.size() > attributeCount; // Children beyond attributes

                    Map<String, Object> result = new HashMap<>();
                    if (hasText == false && hasChildren == false && hasAttributes == false) {
                        // Empty element
                        result.put(elementName, null);
                        return result;
                    } else if (hasText && hasChildren == false) {
                        // Only text content (and possibly attributes)
                        if (hasAttributes) {
                            element.put("#text", trimmedText);
                            result.put(elementName, element);
                            return result;
                        } else {
                            result.put(elementName, trimmedText);
                            return result;
                        }
                    } else if (hasText == false && hasChildren) {
                        // Only child elements (and possibly attributes)
                        result.put(elementName, element);
                        return result;
                    } else {
                        // Both text and children (and possibly attributes)
                        element.put("#text", trimmedText);
                        result.put(elementName, element);
                        return result;
                    }
            }
        }

        return null;
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
            boolean ignoreEmptyValue = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_empty_value", false);

            return new XmlProcessor(processorTag, description, field, targetField, ignoreMissing, ignoreFailure, toLower, ignoreEmptyValue);
        }
    }
}
