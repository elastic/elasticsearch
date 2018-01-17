/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.analysis.index.MappingException;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.lang.Math.floor;
import static java.lang.Math.log10;
import static java.lang.Math.round;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableSet;

public abstract class Types {

    private static final Set<String> KNOWN_TYPES;

    static {
        Set<String> types = new HashSet<>();
        types.add("text");
        types.add("keyword");
        types.add("long");
        types.add("integer");
        types.add("short");
        types.add("byte");
        types.add("double");
        types.add("float");
        types.add("half_float");
        types.add("scaled_float");
        types.add("date");
        types.add("boolean");
        types.add("binary");
        types.add("object");
        types.add("nested");

        KNOWN_TYPES = unmodifiableSet(types);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, DataType> fromEs(Map<String, Object> asMap) {
        Map<String, Object> props = null;
        if (asMap != null && !asMap.isEmpty()) {
            props = (Map<String, Object>) asMap.get("properties");
        }
        return props == null || props.isEmpty() ? emptyMap() : startWalking(props);
    }

    private static Map<String, DataType> startWalking(Map<String, Object> mapping) {
        Map<String, DataType> types = new LinkedHashMap<>();

        if (mapping == null) {
            return emptyMap();
        }
        for (Entry<String, Object> entry : mapping.entrySet()) {
            walkMapping(entry.getKey(), entry.getValue(), types);
        }

        return types;
    }

    @SuppressWarnings("unchecked")
    private static void walkMapping(String name, Object value, Map<String, DataType> mapping) {
        // object type - only root or nested docs supported
        if (value instanceof Map) {
            Map<String, Object> content = (Map<String, Object>) value;

            // extract field type
            Object type = content.get("type");
            if (type instanceof String) {
                String st = type.toString();

                if (knownType(st)) {
                    if (isNested(st)) {
                        mapping.put(name, new NestedType(fromEs(content)));
                    } else {
                        // check dates first to account for the format
                        DataType primitiveType = createPrimitiveType(st, content);
                        if (primitiveType != null) {
                            mapping.put(name, primitiveType);
                        }
                    }
                } else {
                    mapping.put(name, new UnsupportedDataType(st));
                }
            }
            // object type ?
            else if (type == null && content.containsKey("properties")) {
                mapping.put(name, new ObjectType(fromEs(content)));
            }
            // bail out
            else {
                throw new MappingException("Unsupported mapping %s", type);
            }
        } else {
            throw new MappingException("Unrecognized mapping %s", value);
        }
    }

    @SuppressWarnings("unchecked")
    private static DataType createPrimitiveType(String typeString, Map<String, Object> content) {
        // since this setting is available in most types, search for it regardless

        DataType type = null;

        boolean docValues = boolSetting(content.get("doc_values"), true);
        switch (typeString) {
            case "date":
                Object fmt = content.get("format");
                if (fmt != null) {
                    type = new DateType(docValues, Strings.delimitedListToStringArray(fmt.toString(), "||"));
                }
                else {
                    type = docValues ? DateType.DEFAULT : new DateType(false);
                }
                break;
            case "text":
                boolean fieldData = boolSetting(content.get("fielddata"), false);
                Object value = content.get("fields");
                Map<String, DataType> fields = emptyMap();
                if (value instanceof Map) {
                    fields = startWalking((Map<String, Object>) value);
                }
                type = TextType.from(fieldData, fields);
                break;
            case "keyword":
                int length = intSetting(content.get("ignore_above"), KeywordType.DEFAULT_LENGTH);
                boolean normalized = Strings.hasText(textSetting(content.get("normalizer"), null));
                fields = emptyMap();
                value = content.get("fields");
                if (value instanceof Map) {
                    fields = startWalking((Map<String, Object>) value);
                }
                type = KeywordType.from(docValues, length, normalized, fields);
                break;
            default:
                type = DataTypes.fromEsName(typeString, docValues);
        }

        return type;
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

    private static boolean knownType(String st) {
        return KNOWN_TYPES.contains(st);
    }

    private static boolean isNested(String type) {
        return "nested".equals(type);
    }

    static int precision(long number) {
        long abs = number == Long.MIN_VALUE ? Long.MAX_VALUE : number < 0 ? -number : number;
        return (int) round(floor(log10(abs))) + 1;
    }
}