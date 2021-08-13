/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.common.Strings;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;

public abstract class Types {

    @SuppressWarnings("unchecked")
    public static Map<String, EsField> fromEs(DataTypeRegistry typeRegistry, Map<String, Object> asMap) {
        Map<String, Object> props = null;
        if (asMap != null && asMap.isEmpty() == false) {
            props = (Map<String, Object>) asMap.get("properties");
        }
        return props == null || props.isEmpty() ? emptyMap() : startWalking(typeRegistry, props);
    }

    private static Map<String, EsField> startWalking(DataTypeRegistry typeRegistry, Map<String, Object> mapping) {
        Map<String, EsField> types = new LinkedHashMap<>();

        if (mapping == null) {
            return emptyMap();
        }
        for (Entry<String, Object> entry : mapping.entrySet()) {
            walkMapping(typeRegistry, entry.getKey(), entry.getValue(), types);
        }

        return types;
    }

    private static DataType getType(DataTypeRegistry typeRegistry, Map<String, Object> content) {
        if (content.containsKey("type")) {
            String typeName = content.get("type").toString();
            if ("constant_keyword".equals(typeName) || "wildcard".equals(typeName)) {
                return KEYWORD;
            }
            try {
                return typeRegistry.fromEs(typeName);
            } catch (IllegalArgumentException ex) {
                return UNSUPPORTED;
            }
        } else if (content.containsKey("properties")) {
            return OBJECT;
        } else {
            return UNSUPPORTED;
        }
    }

    @SuppressWarnings("unchecked")
    private static void walkMapping(DataTypeRegistry typeRegistry, String name, Object value, Map<String, EsField> mapping) {
        // object type - only root or nested docs supported
        if (value instanceof Map) {
            Map<String, Object> content = (Map<String, Object>) value;

            // extract field type
            DataType esDataType = getType(typeRegistry, content);
            final Map<String, EsField> properties;
            if (esDataType == OBJECT || esDataType == NESTED) {
                properties = fromEs(typeRegistry, content);
            } else if (content.containsKey("fields")) {
                // Check for multifields
                Object fields = content.get("fields");
                if (fields instanceof Map) {
                    properties = startWalking(typeRegistry, (Map<String, Object>) fields);
                } else {
                    properties = Collections.emptyMap();
                }
            } else {
                properties = fromEs(typeRegistry, content);
            }
            boolean docValues = boolSetting(content.get("doc_values"), esDataType.hasDocValues());
            final EsField field;
            if (esDataType == TEXT) {
                field = new TextEsField(name, properties, docValues);
            } else if (esDataType == KEYWORD) {
                int length = intSetting(content.get("ignore_above"), Short.MAX_VALUE);
                boolean normalized = Strings.hasText(textSetting(content.get("normalizer"), null));
                field = new KeywordEsField(name, properties, docValues, length, normalized);
            } else if (esDataType == DATETIME) {
                field = DateEsField.dateEsField(name, properties, docValues);
            } else if (esDataType == UNSUPPORTED) {
                String type = content.get("type").toString();
                field = new UnsupportedEsField(name, type, null, properties);
                propagateUnsupportedType(name, type, properties);
            } else {
                field = new EsField(name, esDataType, properties, docValues);
            }
            mapping.put(name, field);
        } else {
            throw new IllegalArgumentException("Unrecognized mapping " + value);
        }
    }

    private static String textSetting(Object value, String defaultValue) {
        return value == null ? defaultValue : value.toString();
    }

    private static boolean boolSetting(Object value, boolean defaultValue) {
        return value == null ? defaultValue : Booleans.parseBoolean(value.toString(), defaultValue);
    }

    private static int intSetting(Object value, int defaultValue) {
        return value == null ? defaultValue : Integer.parseInt(value.toString());
    }

    private static void propagateUnsupportedType(String inherited, String originalType, Map<String, EsField> properties) {
        if (properties != null && properties.isEmpty() == false) {
            for (Entry<String, EsField> entry : properties.entrySet()) {
                EsField field = entry.getValue();
                UnsupportedEsField u;
                if (field instanceof UnsupportedEsField) {
                    u = (UnsupportedEsField) field;
                    u = new UnsupportedEsField(u.getName(), originalType, inherited, u.getProperties());
                } else {
                    u = new UnsupportedEsField(field.getName(), originalType, inherited, field.getProperties());
                }
                entry.setValue(u);
                propagateUnsupportedType(inherited, originalType, u.getProperties());
            }
        }
    }
}
