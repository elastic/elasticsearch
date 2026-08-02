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
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.junit.Assert.assertNotNull;

public class LoadMapping {
    public static Map<String, EsField> loadMapping(String name) {
        var path = "/index/mappings/" + name;
        InputStream stream = LoadMapping.class.getResourceAsStream(path);
        assertNotNull("Could not find mapping resource:" + path, stream);
        return loadMapping(stream);
    }

    public static Map<String, EsField> loadMapping(InputStream in) {
        try (in) {
            return fromEs(XContentHelper.convertToMap(JsonXContent.jsonXContent, in, true));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Map<String, EsField> fromEs(Map<String, Object> asMap) {
        return fromEs(asMap, false);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, EsField> fromEs(Map<String, Object> asMap, boolean inheritDimension) {
        Map<String, Object> props = null;
        if (asMap != null && asMap.isEmpty() == false) {
            props = (Map<String, Object>) asMap.get("properties");
        }
        return props == null || props.isEmpty() ? emptyMap() : startWalking(props, inheritDimension);
    }

    private static Map<String, EsField> startWalking(Map<String, Object> mapping, boolean inheritDimension) {
        Map<String, EsField> types = new LinkedHashMap<>();

        if (mapping == null) {
            return emptyMap();
        }
        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            walkMapping(entry.getKey(), entry.getValue(), types, inheritDimension);
        }

        return types;
    }

    @SuppressWarnings("unchecked")
    private static void walkMapping(String name, Object value, Map<String, EsField> mapping, boolean inheritDimension) {
        // object type - only root or nested docs supported
        if (value instanceof Map) {
            Map<String, Object> content = (Map<String, Object>) value;

            if ("nested".equals(content.get("type"))) {
                // Nested fields are entirely removed by IndexResolver so we mimic it.
                return;
            }
            // A `passthrough` object exposes its leaf subfields under their declared (scalar) types, mirroring how
            // production field-caps resolves them: the object itself is not a usable scalar, but e.g. `labels.zone`
            // is a plain keyword. When the passthrough carries `time_series_dimension`, every leaf below it is a
            // dimension. Modelling this here (instead of treating the whole subtree as UNSUPPORTED) lets tests read
            // and group by Prometheus/OTel labels exactly as a real cluster does.
            boolean isPassthrough = "passthrough".equals(content.get("type"));
            // extract field type
            DataType esDataType = isPassthrough ? OBJECT : getType(content);
            boolean explicitDimension = boolSetting(content.get("time_series_dimension"), false);
            // Dimension-ness applies to leaf fields and, for a passthrough (or an object nested beneath one), flows
            // down to its leaves; the container object node itself is never a groupable dimension.
            boolean dimensionScope = explicitDimension || inheritDimension;
            boolean isDimension = dimensionScope && esDataType != OBJECT;
            boolean childInherit = isPassthrough || (esDataType == OBJECT && inheritDimension);
            final Map<String, EsField> properties;
            if (esDataType == OBJECT) {
                properties = fromEs(content, childInherit && dimensionScope);
            } else if (content.containsKey("fields")) {
                // Check for multifields
                Object fields = content.get("fields");
                if (fields instanceof Map) {
                    properties = startWalking((Map<String, Object>) fields, false);
                } else {
                    properties = Collections.emptyMap();
                }
            } else {
                properties = fromEs(content, false);
            }
            boolean docValues = boolSetting(content.get("doc_values"), esDataType.hasDocValues());
            boolean isMetric = content.containsKey("time_series_metric");
            if (isDimension && isMetric) {
                throw new IllegalStateException("Field configured as both dimension and metric:" + value);
            }
            EsField.TimeSeriesFieldType tsType = EsField.TimeSeriesFieldType.NONE;
            if (isDimension) {
                tsType = EsField.TimeSeriesFieldType.DIMENSION;
            }
            if (isMetric) {
                tsType = EsField.TimeSeriesFieldType.METRIC;
            }
            final EsField field;
            if (esDataType == TEXT) {
                field = new TextEsField(name, properties, docValues, false, tsType);
            } else if (esDataType == KEYWORD) {
                int length = intSetting(content.get("ignore_above"), Short.MAX_VALUE);
                boolean normalized = Strings.hasText(textSetting(content.get("normalizer"), null));
                field = new KeywordEsField(name, properties, docValues, length, normalized, false, tsType);
            } else if (esDataType == DATETIME) {
                field = DateEsField.dateEsField(name, properties, docValues, tsType);
            } else if (esDataType == UNSUPPORTED) {
                String type = content.get("type").toString();
                field = new UnsupportedEsField(name, List.of(type), null, properties);
                propagateUnsupportedType(name, type, properties);
            } else {
                field = new EsField(name, esDataType, properties, docValues, tsType);
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
                    u = new UnsupportedEsField(u.getName(), List.of(originalType), inherited, u.getProperties());
                } else {
                    u = new UnsupportedEsField(field.getName(), List.of(originalType), inherited, field.getProperties());
                }
                entry.setValue(u);
                propagateUnsupportedType(inherited, originalType, u.getProperties());
            }
        }
    }
}
