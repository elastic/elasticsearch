/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.cluster.stats.MappingVisitor;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.LinkedHashMap;
import java.util.Map;

public class LegacyMappingConverter {

    public Map<String, Object> extractRuntimeFieldMapping(Map<String, ?> mapping) {
        final Map<String, Object> result = new LinkedHashMap<>();
        MappingVisitor.visitMapping(mapping, (key, map) -> {
            Object type = map.get("type");
            if (type instanceof String) {
                String typeAsString = (String) type;
                // copy over known basic types for which there are runtime fields
                // first auto-convert some types
                switch (typeAsString) {
                    case "byte":
                    case "short":
                    case "integer":
                        typeAsString = "long";
                        break;
                    case "half_float":
                    case "float":
                        typeAsString = "double";
                        break;
                    default:
                        // ignore
                        break;
                }
                switch (typeAsString) {
                    case "boolean":
                    case "date":
                    case "double":
                    case "geo_point":
                    case "ip":
                    case "keyword":
                    case "long":
                        // copy over only format parameter if it exists
                        // TODO: converting multi-fields to default runtime fields does not make sense
                        //  (as field name in mapping is not equal to field name in _source)
                        //  This means we need to add a script that returns the correct field from _source
                        final Map<String, Object> runtimeFieldDefinition = new LinkedHashMap<>();
                        runtimeFieldDefinition.put("type", typeAsString);
                        Object format = map.get("format");
                        if (format instanceof String) {
                            runtimeFieldDefinition.put("format", format);
                        }
                        result.put(key, runtimeFieldDefinition);
                        break;
                    default:
                        // ignore
                        break;
                }
            }
        });
        return result;
    }

    public Map<String, ?> extractDocValuesMapping(Map<String, ?> mapping) {
        final Map<String, Object> result = new LinkedHashMap<>();
        MappingVisitor.visitMapping(mapping, (key, map) -> {
            Object type = map.get("type");
            if (type instanceof String) {
                // copy over known basic types for which there are doc-value only implementations
                switch ((String) type) {
                    case "date":
                    case "date_nanos":
                    case "long":
                    case "integer":
                    case "short":
                    case "byte":
                    case "double":
                    case "float":
                    case "half_float":
                    case "scaled_float":
                        final Object docValues = map.get("doc_values");
                        // check that doc_values had not been disabled
                        if (docValues != null && XContentMapValues.nodeBooleanValue(docValues) == false) {
                            return;
                        }
                        final Map<String, Object> docValueOnlyDefinition = new LinkedHashMap<>(map);
                        docValueOnlyDefinition.put("index", false);
                        // TODO: this does not work, as the key is "dotted"
                        // we would need to recreate hierarchical mapping
                        result.put(key, docValueOnlyDefinition);
                        break;
                    default:
                        // ignore
                }
            }
        });
        return result;
    }

    public Map<String, ?> extractNewMapping(Map<String, ?> mapping, boolean withTypes) {
        final Map<String, Object> result = new LinkedHashMap<>();
        if (withTypes) {
            for (Map.Entry<String, ?> entry : mapping.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, ?> actualMapping = (Map<String, ?>) entry.getValue();
                result.put(entry.getKey(), extractNewMapping(actualMapping, false));
            }
            return result;
        }

        Object properties = mapping.get("properties");
        if (properties instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> propertiesAsMap = (Map<String, ?>) properties;
            Map<String, Object> newPropertiesMap = new LinkedHashMap<>();
            for (Map.Entry<String, ?> entry : propertiesAsMap.entrySet()) {
                final Object v = entry.getValue();
                if (v instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> fieldMapping = (Map<String, ?>) v;
                    Object type = fieldMapping.get("type");
                    Map<String, Object> fieldMap = new LinkedHashMap<>();
                    if (type instanceof String) {
                        // copy over known types
                        switch ((String) type) {
                            case "binary":
                            case "boolean":
                            case "keyword":
                            case "long":
                            case "integer":
                            case "short":
                            case "byte":
                            case "double":
                            case "float":
                            case "half_float":
                            case "scaled_float":
                            case "unsigned_long":
                            case "date":
                            case "date_nanos":
                            case "alias":
                            case "object":
                            case "flattened":
                            case "nested":
                            case "join":
                            case "integer_range":
                            case "float_range":
                            case "long_range":
                            case "double_range":
                            case "date_range":
                            case "ip_range":
                            case "ip":
                            case "version":
                            case "murmur3":
                            case "aggregate_metric_double":
                            case "histogram":
                            case "text":
                            case "match_only_text":
                            case "annotated-text":
                            case "completion":
                            case "search_as_you_type":
                            case "token_count":
                            case "dense_vector":
                            case "sparse_vector":
                            case "rank_feature":
                            case "rank_features":
                            case "geo_point":
                            case "geo_shape":
                            case "point":
                            case "shape":
                            case "percolator":
                                fieldMap.putAll(fieldMapping);
                                break;
                            default:
                                assert false;
                                // ignore
                        }
                    }

                    // Multi fields
                    Object fieldsO = fieldMapping.get("fields");
                    if (fieldsO instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> fields = (Map<String, ?>) fieldsO;
                        Map<String, Object> fieldsMap = new LinkedHashMap<>();
                        for (Map.Entry<String, ?> subfieldEntry : fields.entrySet()) {
                            Object v2 = subfieldEntry.getValue();
                            if (v2 instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, ?> fieldMapping2 = (Map<String, ?>) v2;
                                String key = entry.getKey() + "." + subfieldEntry.getKey();
                                Map<String, ?> extracted = extractNewMapping(fieldMapping2, false);
                                if (extracted.isEmpty() == false) {
                                    fieldsMap.put(key, extracted);
                                }
                            }
                        }
                        if (fieldsMap.isEmpty() == false) {
                            fieldMap.put("fields", fieldsMap);
                        }
                    }

                    Map<String, ?> extracted = extractNewMapping(fieldMapping, false);
                    fieldMap.putAll(extracted);

                    if (fieldMap.isEmpty() == false) {
                        newPropertiesMap.put(entry.getKey(), fieldMap);
                    }
                }
            }
            if (newPropertiesMap.isEmpty() == false) {
                result.put("properties", newPropertiesMap);
            }
        }

        return result;
    }
}
