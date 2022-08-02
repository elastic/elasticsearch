/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.search;

import org.elasticsearch.cluster.metadata.MappingMetadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class XSearchQueryOptions {

    private final static Map<String, Float> DEFAULT_FIELD_WEIGHTS = new HashMap<>();

    static {
        DEFAULT_FIELD_WEIGHTS.put(".stem", 0.95f);
        DEFAULT_FIELD_WEIGHTS.put(".prefix", 0.1f);
        DEFAULT_FIELD_WEIGHTS.put(".joined", 0.75f);
        DEFAULT_FIELD_WEIGHTS.put(".delimiter", 0.4f);
    }

    private final String query;
    private final Set<String> searchFields;
    private String minimumShouldMatch = "1<-1 3<49%";

    public XSearchQueryOptions(String query, Collection<MappingMetadata> mappingsMetadata) {
        this.query = query;
        this.searchFields = mappingsMetadata
            .stream()
            .flatMap(indexMetadata -> getSearchFieldsFromMetadata(indexMetadata).stream())
            .collect(Collectors.toSet());
    }

    public String getQuery() {
        return query;
    }

    public Float getBoostForField(String field) {
        int index = field.lastIndexOf('.');
        if (index != -1) {
            String suffix = field.substring(index);
            return DEFAULT_FIELD_WEIGHTS.getOrDefault(suffix,1.0f);
        }

        return 1.0f;
    }

    public String getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    private static Set<String> getSearchFieldsFromMetadata(MappingMetadata mappingMetadata) {
        @SuppressWarnings("unchecked")
        final Set<String> searchFields = getSearchFieldsFromFieldMapping((Map<String, Map<String, Object>>) mappingMetadata.getSourceAsMap().get("properties"), "");
        return searchFields;
    }

    private static Set<String> getSearchFieldsFromFieldMapping(Map<String, Map<String, Object>> fieldProperties, String prefix) {
        Set<String> fields = new HashSet<>();

        for (Map.Entry<String, Map<String, Object>> entry : fieldProperties.entrySet()) {
            final Map<String, Object> fieldMapping = entry.getValue();
            final String fieldName = entry.getKey();
            if ("text".equals(fieldMapping.get("type"))) {
                fields.add(prefix + fieldName);
            }
            if (fieldMapping.containsKey("fields")) {
                @SuppressWarnings("unchecked")
                final Set<String> subFields = getSearchFieldsFromFieldMapping(
                    (Map<String, Map<String, Object>>) fieldMapping.get("fields"),
                    prefix + fieldName + "."
                );
                fields.addAll(subFields);
            }
        }

        return fields;
    }

    public Set<String> getSearchFields() {
        return searchFields;
    }
}
