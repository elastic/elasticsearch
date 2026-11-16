/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Which fields of a mapping hold several values for a document, so that they become Parquet lists. A field decides with its
 * {@code multi_value} parameter, and a field without one takes the index default {@code index.mapping.doc_values.multi_value} (the
 * provider of the format sets that to {@code false}, so a field is flat unless it opts in). The mapper exposes the parameter only through
 * a protected method, so this reads it from the source of the mapping, which holds the parameters that the user wrote.
 */
public final class MultiValuedFields {

    private MultiValuedFields() {}

    /** Whether the definition of a leaf field says {@code multi_value: true}, with the index default for a field that says nothing. */
    public static boolean isMultiValued(Map<?, ?> definition, boolean indexDefault) {
        Object value = definition.get("multi_value");
        if (value == null && definition.get("doc_values") instanceof Map<?, ?> docValues) {
            value = docValues.get("multi_value"); // the nested form of the parameter
        }
        return value == null ? indexDefault : Booleans.parseBoolean(String.valueOf(value));
    }

    /** The full paths of the multi-valued leaf fields of the mapping. */
    public static Set<String> of(CompressedXContent mappingSource, boolean indexDefault) {
        final Map<String, Object> root = XContentHelper.convertToMap(mappingSource.uncompressed(), false, XContentType.JSON).v2();
        final Set<String> fields = new HashSet<>();
        if (rootOf(root).get("properties") instanceof Map<?, ?> properties) {
            collect("", properties, indexDefault, fields);
        }
        return fields;
    }

    /** A mapping is the body of the type, or the body under the name of the type ({@code _doc}). */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> rootOf(Map<String, Object> mapping) {
        if (mapping.containsKey("properties") == false && mapping.size() == 1) {
            final Object only = mapping.values().iterator().next();
            if (only instanceof Map<?, ?> body) {
                return (Map<String, Object>) body;
            }
        }
        return mapping;
    }

    private static void collect(String prefix, Map<?, ?> properties, boolean indexDefault, Set<String> fields) {
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?> definition) {
                final String path = prefix + entry.getKey();
                final Object type = definition.get("type");
                if (type == null || "object".equals(type)) {
                    if (definition.get("properties") instanceof Map<?, ?> children) {
                        collect(path + ".", children, indexDefault, fields);
                    }
                } else if (isMultiValued(definition, indexDefault)) {
                    fields.add(path);
                }
            }
        }
    }
}
