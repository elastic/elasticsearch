/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.elasticsearch.ingest.IngestDocument;

final class CefParser {

    private final IngestDocument ingestDocument;
    private final boolean removeEmptyValue;

    CefParser(IngestDocument ingestDocument, boolean removeEmptyValue) {
        this.ingestDocument = ingestDocument;
        this.removeEmptyValue = removeEmptyValue;
    }

    // Existing patterns...
    private static final Pattern HEADER_PATTERN = Pattern.compile("(?:\\\\\\||\\\\\\\\|[^|])*?");
    private static final Pattern HEADER_NEXT_FIELD_PATTERN = Pattern.compile("(" + HEADER_PATTERN.pattern() + ")\\|");
    private static final Pattern HEADER_ESCAPE_CAPTURE = Pattern.compile("\\\\([\\\\|])");

    // New patterns for extension parsing
    private static final String EXTENSION_KEY_PATTERN = "(?:\\w+(?:\\.[^\\.=\\s\\|\\\\\\[\\]]+)*(?:\\[[0-9]+\\])?(?==))";
    private static final Pattern EXTENSION_KEY_ARRAY_CAPTURE = Pattern.compile("^([^\\[\\]]+)((?:\\[[0-9]+\\])+)$");
    private static final String EXTENSION_VALUE_PATTERN = "(?:\\S|\\s(?!" + EXTENSION_KEY_PATTERN + "=))*";
    private static final Pattern EXTENSION_NEXT_KEY_VALUE_PATTERN = Pattern
            .compile("(" + EXTENSION_KEY_PATTERN + ")=(" + EXTENSION_VALUE_PATTERN + ")(?:\\s+|$)");

    private static final Map<String, String> HEADER_FIELD_SANITIZER_MAPPING = new HashMap<>();
    private static final Map<String, String> EXTENSION_VALUE_SANITIZER_MAPPING = new HashMap<>();
    private static final Map<String, String> EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING = new HashMap<>();

    private static final Map<String, String> FIELD_MAPPING = new HashMap<>();
    private static final Map<String, String> DECODE_MAPPING = new HashMap<>();

    static {
        HEADER_FIELD_SANITIZER_MAPPING.put("\\", "\\\\");
        HEADER_FIELD_SANITIZER_MAPPING.put("|", "\\|");
        HEADER_FIELD_SANITIZER_MAPPING.put("\n", " ");
        HEADER_FIELD_SANITIZER_MAPPING.put("\r", " ");

        EXTENSION_VALUE_SANITIZER_MAPPING.put("\\", "\\\\");
        EXTENSION_VALUE_SANITIZER_MAPPING.put("=", "\\=");
        EXTENSION_VALUE_SANITIZER_MAPPING.put("\n", "\\n");
        EXTENSION_VALUE_SANITIZER_MAPPING.put("\r", "\\n");

        EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING.put("\\\\", "\\");
        EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING.put("\\=", "=");
        EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING.put("\\n", "\n");
        EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING.put("\\r", "\n");

        FIELD_MAPPING.put("src", "source.ip");
        FIELD_MAPPING.put("spt", "source.port");
        FIELD_MAPPING.put("dst", "destination.ip");
        FIELD_MAPPING.put("dpt", "destination.port");
        FIELD_MAPPING.put("suser", "source.user.name");
        FIELD_MAPPING.put("duser", "destination.user.name");
        // Add more mappings as needed

        // Initialize decode mappings
        DECODE_MAPPING.put("src", "sourceAddress");
        DECODE_MAPPING.put("dst", "destinationAddress");
        DECODE_MAPPING.put("spt", "sourcePort");
        DECODE_MAPPING.put("dpt", "destinationPort");
    }

    void process(String cefString) {
        List<String> headerFields = new ArrayList<>();
        Matcher headerMatcher = HEADER_NEXT_FIELD_PATTERN.matcher(cefString);
        int extensionStart = 0;

        for (int i = 0; i < 7 && headerMatcher.find(); i++) {
            String field = headerMatcher.group(1);
            field = HEADER_ESCAPE_CAPTURE.matcher(field).replaceAll("$1");
            headerFields.add(field);
            extensionStart = headerMatcher.end();
        }

        if (headerFields.size() != 7 || !headerFields.get(0).startsWith("CEF:")) {
            throw new IllegalArgumentException("Invalid CEF format");
        }

        CEFEvent event = new CEFEvent();
        event.setVersion(headerFields.get(0).substring(4));
        event.setDeviceVendor(headerFields.get(1));
        event.setDeviceProduct(headerFields.get(2));
        event.setDeviceVersion(headerFields.get(3));
        event.setDeviceEventClassId(headerFields.get(4));
        event.setName(headerFields.get(5));
        event.setSeverity(headerFields.get(6));

        String extensionString = cefString.substring(extensionStart);
        Map<String, String> extensions = parseExtensions(extensionString);

        if (removeEmptyValue) {
            extensions = removeEmptyValue(extensions);
        }

        event.setExtensions(extensions);

        Map<String, String> translatedFields = new HashMap<>();
        for (Map.Entry<String, String> entry : extensions.entrySet()) {
            String translatedKey = FIELD_MAPPING.getOrDefault(entry.getKey(), entry.getKey());
            translatedFields.put(translatedKey, entry.getValue());
        }
        event.setTranslatedFields(translatedFields);

        ingestDocument.setFieldValue("cef", event.toObject());
    }

    private Map<String, String> parseExtensions(String extensionString) {
        Map<String, String> extensions = new HashMap<>();
        Matcher matcher = EXTENSION_NEXT_KEY_VALUE_PATTERN.matcher(extensionString);
        int lastEnd = 0;
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2);

            // Expand abbreviated extension field keys
            key = DECODE_MAPPING.getOrDefault(key, key);

            // Convert extension field name to strict legal field_reference
            if (key.endsWith("]")) {
                key = convertArrayLikeKey(key);
            }

            extensions.put(key, desanitizeExtensionVal(value.trim()));
            lastEnd = matcher.end();
        }
        // If there's any remaining unparsed content, throw an exception
        if (lastEnd < extensionString.length()) {
            throw new IllegalArgumentException("Invalid extensions; keyless value present: " +
                    extensionString.substring(lastEnd));
        }
        return extensions;
    }

    private Map<String, String> removeEmptyValue(Map<String, String> map) {
        map.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        return map;
    }

    private String convertArrayLikeKey(String key) {
        Matcher matcher = EXTENSION_KEY_ARRAY_CAPTURE.matcher(key);
        if (matcher.matches()) {
            return "[" + matcher.group(1) + "]" + matcher.group(2);
        }
        return key;
    }

    public static String sanitizeExtensionKey(String value) {
        return value.replaceAll("[^a-zA-Z0-9]", "");
    }

    public static String sanitizeExtensionVal(String value) {
        String sanitized = value.replace("\r\n", "\n");
        for (Map.Entry<String, String> entry : EXTENSION_VALUE_SANITIZER_MAPPING.entrySet()) {
            sanitized = sanitized.replace(entry.getKey(), entry.getValue());
        }
        return sanitized;
    }

    public static String desanitizeExtensionVal(String value) {
        String desanitized = value;
        for (Map.Entry<String, String> entry : EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING.entrySet()) {
            desanitized = desanitized.replace(entry.getKey(), entry.getValue());
        }
        return desanitized;
    }

    public static String sanitizeHeaderField(String field) {
        StringBuilder result = new StringBuilder();
        for (char c : field.toCharArray()) {
            String replacement = HEADER_FIELD_SANITIZER_MAPPING.get(String.valueOf(c));
            result.append(replacement != null ? replacement : c);
        }
        return result.toString();
    }

    public static String sanitizeExtensionValue(String value) {
        StringBuilder result = new StringBuilder();
        for (char c : value.toCharArray()) {
            String replacement = EXTENSION_VALUE_SANITIZER_MAPPING.get(String.valueOf(c));
            result.append(replacement != null ? replacement : c);
        }
        return result.toString();
    }

    public static class CEFEvent {
        private String version;
        private String deviceVendor;
        private String deviceProduct;
        private String deviceVersion;
        private String deviceEventClassId;
        private String name;
        private String severity;
        private Map<String, String> extensions;
        private Map<String, String> translatedFields;

        // Getters and setters for all fields
        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getDeviceVendor() {
            return deviceVendor;
        }

        public void setDeviceVendor(String deviceVendor) {
            this.deviceVendor = deviceVendor;
        }

        public String getDeviceProduct() {
            return deviceProduct;
        }

        public void setDeviceProduct(String deviceProduct) {
            this.deviceProduct = deviceProduct;
        }

        public String getDeviceVersion() {
            return deviceVersion;
        }

        public void setDeviceVersion(String deviceVersion) {
            this.deviceVersion = deviceVersion;
        }

        public String getDeviceEventClassId() {
            return deviceEventClassId;
        }

        public void setDeviceEventClassId(String deviceEventClassId) {
            this.deviceEventClassId = deviceEventClassId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSeverity() {
            return severity;
        }

        public void setSeverity(String severity) {
            this.severity = severity;
        }

        public Map<String, String> getExtensions() {
            return extensions;
        }

        public void setExtensions(Map<String, String> extensions) {
            this.extensions = extensions;
        }

        public Map<String, String> getTranslatedFields() {
            return translatedFields;
        }

        public void setTranslatedFields(Map<String, String> translatedFields) {
            this.translatedFields = translatedFields;
        }

        public Object toObject() {
            Map<String, Object> event = new HashMap<>();
            event.put("version", version);
            event.put("deviceVendor", deviceVendor);
            event.put("deviceProduct", deviceProduct);
            event.put("deviceVersion", deviceVersion);
            event.put("deviceEventClassId", deviceEventClassId);
            event.put("name", name);
            event.put("severity", severity);
            event.put("extensions", extensions);
            event.put("translatedFields", translatedFields);
            return event;
        }

    }
}
