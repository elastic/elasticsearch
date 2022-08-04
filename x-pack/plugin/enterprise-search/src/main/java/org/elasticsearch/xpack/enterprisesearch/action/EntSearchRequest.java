/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EntSearchRequest extends ActionRequest implements IndicesRequest, ToXContentObject {

    private final static Map<String, Float> DEFAULT_FIELD_WEIGHTS = new HashMap<>();

    static {
        DEFAULT_FIELD_WEIGHTS.put(".stem", 0.95f);
        DEFAULT_FIELD_WEIGHTS.put(".prefix", 0.1f);
        DEFAULT_FIELD_WEIGHTS.put(".joined", 0.75f);
        DEFAULT_FIELD_WEIGHTS.put(".delimiter", 0.4f);
    }

    private String minimumShouldMatch = "1<-1 3<49%";

    private String index;
    private String query;
    private Set<String> searchFields;

    public EntSearchRequest() {
    }

    public EntSearchRequest(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        query = in.readString();
    }

    private static Set<String> getSearchFieldsFromMetadata(MappingMetadata mappingMetadata) {
        @SuppressWarnings("unchecked") final Set<String> searchFields = getSearchFieldsFromFieldMapping((Map<String, Map<String, Object>>) mappingMetadata.getSourceAsMap().get("properties"), "");
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
                @SuppressWarnings("unchecked") final Set<String> subFields = getSearchFieldsFromFieldMapping(
                    (Map<String, Map<String, Object>>) fieldMapping.get("fields"),
                    prefix + fieldName + "."
                );
                fields.addAll(subFields);
            }
        }

        return fields;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder
            .startObject()
            .field("query", query)
            .field("index", index)
            .endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(query);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null || index.isEmpty()) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (query == null || query.isEmpty()) {
            validationException = addValidationError("query is missing", validationException);
        }

        return validationException;
    }

    public void setFieldsFromFieldMapping(Collection<MappingMetadata> mappingsMetadata) {
        this.searchFields = mappingsMetadata
            .stream()
            .flatMap(indexMetadata -> getSearchFieldsFromMetadata(indexMetadata).stream())
            .collect(Collectors.toSet());
    }

    public Set<String> getSearchFields() {
        return searchFields;
    }

    public void setSearchFields(Set<String> searchFields) {
        this.searchFields = searchFields;
    }

    public String getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    public Float getBoostForField(String field) {
        int index = field.lastIndexOf('.');
        if (index != -1) {
            String suffix = field.substring(index);
            return DEFAULT_FIELD_WEIGHTS.getOrDefault(suffix, 1.0f);
        }

        return 1.0f;
    }

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.STRICT_EXPAND_OPEN_CLOSED_HIDDEN;
    }
}
