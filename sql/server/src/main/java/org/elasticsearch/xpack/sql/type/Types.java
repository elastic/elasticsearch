/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.analysis.catalog.MappingException;

import static java.lang.Math.floor;
import static java.lang.Math.log10;
import static java.lang.Math.round;
import static java.util.Collections.emptyMap;

public abstract class Types {

    @SuppressWarnings("unchecked")
    public static Map<String, DataType> fromEs(Map<String, Object> asMap) {
        return startWalking((Map<String, Object>) asMap.get("properties"));
    }

    private static Map<String, DataType> startWalking(Map<String, Object> mapping) {
        Map<String, DataType> translated = new LinkedHashMap<>();

        for (Entry<String, Object> entry : mapping.entrySet()) {
            walkMapping(entry.getKey(), entry.getValue(), translated);
        }

        return translated;
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

                if (isNested(st)) {
                    mapping.put(name, new NestedType(fromEs(content)));
                    return;
                }

                if (isPrimitive(st)) {
                    // check dates first to account for the format
                    mapping.put(name, createPrimitiveType(st, content));
                    return;
                }

                else {
                    throw new MappingException("Don't know how to parse entry %s in map %s", type, content);
                }
            }

            // object type ignored
        }
        else {
            throw new MappingException("Don't know how to parse mapping %s", value);
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
                    type = new DateType(docValues, Strings.split(fmt.toString(), "||"));
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
                fields = emptyMap();
                value = content.get("fields");
                if (value instanceof Map) {
                    fields = startWalking((Map<String, Object>) value);
                }
                type = KeywordType.from(docValues, length, fields);
                break;
            default:
                type = DataTypes.fromEsName(typeString, docValues);
        }

        return type;
    }

    private static boolean boolSetting(Object value, boolean defaultValue) {
        return value == null ? defaultValue : Booleans.parseBoolean(value.toString(), defaultValue);
    }

    private static int intSetting(Object value, int defaultValue) {
        return value == null ? defaultValue : Integer.parseInt(value.toString());
    }

    private static boolean isPrimitive(String string) {
        return !isNested(string);
    }

    private static boolean isNested(String type) {
        return "nested".equals(type);
    }

    static int precision(long number) {
        long abs = number == Long.MIN_VALUE ? Long.MAX_VALUE : number < 0 ? -number : number;
        return (int) round(floor(log10(abs))) + 1;
    }
}