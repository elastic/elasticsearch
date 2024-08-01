/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NESTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.junit.Assert.assertNotNull;

public class LoadMapping {
    public static Map<String, EsField> loadMapping(String name) {
        InputStream stream = LoadMapping.class.getResourceAsStream("/" + name);
        assertNotNull("Could not find mapping resource:" + name, stream);
        return loadMapping(stream);
    }

    private static Map<String, EsField> loadMapping(InputStream stream) {
        try (InputStream in = stream) {
            Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, in, true);
            return fromEs(map);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, EsField> fromEs(Map<String, Object> asMap) {
        Map<String, Object> props = null;
        if (asMap != null && asMap.isEmpty() == false) {
            props = (Map<String, Object>) asMap.get("properties");
        }
        return props == null || props.isEmpty() ? emptyMap() : startWalking(props);
    }

    private static Map<String, EsField> startWalking(Map<String, Object> mapping) {
        Map<String, EsField> types = new LinkedHashMap<>();

        if (mapping == null) {
            return emptyMap();
        }
        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            walkMapping(entry.getKey(), entry.getValue(), types);
        }

        return types;
    }

    @SuppressWarnings("unchecked")
    private static void walkMapping(String name, Object value, Map<String, EsField> mapping) {
        // object type - only root or nested docs supported
        if (value instanceof Map) {
            Map<String, Object> content = (Map<String, Object>) value;

            // extract field type
            DataType esDataType = getType(content);
            final Map<String, EsField> properties;
            if (esDataType == OBJECT || esDataType == NESTED) {
                properties = fromEs(content);
            } else if (content.containsKey("fields")) {
                // Check for multifields
                Object fields = content.get("fields");
                if (fields instanceof Map) {
                    properties = startWalking((Map<String, Object>) fields);
                } else {
                    properties = Collections.emptyMap();
                }
            } else {
                properties = fromEs(content);
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

    private static DataType getType(Map<String, Object> content) {
        if (content.containsKey("type")) {
            String typeName = content.get("type").toString();
            if ("constant_keyword".equals(typeName) || "wildcard".equals(typeName)) {
                return KEYWORD;
            }
            final Object metricsTypeParameter = content.get(TimeSeriesParams.TIME_SERIES_METRIC_PARAM);
            final TimeSeriesParams.MetricType metricType;
            if (metricsTypeParameter instanceof String str) {
                metricType = TimeSeriesParams.MetricType.fromString(str);
            } else {
                metricType = (TimeSeriesParams.MetricType) metricsTypeParameter;
            }
            try {
                return EsqlDataTypeRegistry.INSTANCE.fromEs(typeName, metricType);
            } catch (IllegalArgumentException ex) {
                return UNSUPPORTED;
            }
        } else if (content.containsKey("properties")) {
            return OBJECT;
        } else {
            return UNSUPPORTED;
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

    public static void propagateUnsupportedType(String inherited, String originalType, Map<String, EsField> properties) {
        if (properties != null && properties.isEmpty() == false) {
            for (Map.Entry<String, EsField> entry : properties.entrySet()) {
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
