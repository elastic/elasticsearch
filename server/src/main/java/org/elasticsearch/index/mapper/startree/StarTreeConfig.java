/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.startree;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the complete star-tree configuration for an index mapping.
 * The configuration can contain multiple named star-tree definitions.
 */
public final class StarTreeConfig implements Writeable, ToXContentObject {

    public static final int DEFAULT_MAX_LEAF_DOCS = 10000;
    public static final int DEFAULT_STAR_NODE_THRESHOLD = 10000;

    private final Map<String, StarTreeFieldConfig> starTrees;

    public StarTreeConfig(Map<String, StarTreeFieldConfig> starTrees) {
        if (starTrees == null || starTrees.isEmpty()) {
            throw new IllegalArgumentException("Star-tree configuration must have at least one star-tree definition");
        }
        this.starTrees = Collections.unmodifiableMap(new LinkedHashMap<>(starTrees));
    }

    public StarTreeConfig(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, StarTreeFieldConfig> trees = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            trees.put(name, new StarTreeFieldConfig(in));
        }
        this.starTrees = Collections.unmodifiableMap(trees);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(starTrees.size());
        for (Map.Entry<String, StarTreeFieldConfig> entry : starTrees.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public Map<String, StarTreeFieldConfig> getStarTrees() {
        return starTrees;
    }

    public StarTreeFieldConfig getStarTree(String name) {
        return starTrees.get(name);
    }

    public boolean hasStarTree(String name) {
        return starTrees.containsKey(name);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, StarTreeFieldConfig> entry : starTrees.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        return builder;
    }

    /**
     * Parse star-tree configuration from XContent.
     */
    @SuppressWarnings("unchecked")
    public static StarTreeConfig parse(Map<String, Object> starTreeNode) {
        if (starTreeNode.isEmpty()) {
            throw new MapperParsingException("_star_tree configuration cannot be empty");
        }

        Map<String, StarTreeFieldConfig> starTrees = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : starTreeNode.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map == false) {
                throw new MapperParsingException("Star-tree definition [" + name + "] must be an object");
            }

            StarTreeFieldConfig config = StarTreeFieldConfig.parse((Map<String, Object>) value, name);
            starTrees.put(name, config);
        }

        return new StarTreeConfig(starTrees);
    }

    /**
     * Merge this config with another config. The other config takes precedence.
     */
    public StarTreeConfig merge(StarTreeConfig other) {
        if (other == null) {
            return this;
        }
        Map<String, StarTreeFieldConfig> merged = new LinkedHashMap<>(this.starTrees);
        merged.putAll(other.starTrees);
        return new StarTreeConfig(merged);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarTreeConfig that = (StarTreeConfig) o;
        return Objects.equals(starTrees, that.starTrees);
    }

    @Override
    public int hashCode() {
        return Objects.hash(starTrees);
    }

    @Override
    public String toString() {
        return "StarTreeConfig{starTrees=" + starTrees.keySet() + "}";
    }

    /**
     * Configuration for a single star-tree field.
     */
    public static final class StarTreeFieldConfig implements Writeable, ToXContentObject {

        private static final String GROUPING_FIELDS_PARAM = "grouping_fields";
        private static final String VALUES_PARAM = "values";
        private static final String CONFIG_PARAM = "config";
        private static final String MAX_LEAF_DOCS_PARAM = "max_leaf_docs";
        private static final String STAR_NODE_THRESHOLD_PARAM = "star_node_threshold";

        private final String name;
        private final List<StarTreeGroupingField> groupingFields;
        private final List<StarTreeValue> values;
        private final int maxLeafDocs;
        private final int starNodeThreshold;

        public StarTreeFieldConfig(
            String name,
            List<StarTreeGroupingField> groupingFields,
            List<StarTreeValue> values,
            int maxLeafDocs,
            int starNodeThreshold
        ) {
            if (Strings.isNullOrEmpty(name)) {
                throw new IllegalArgumentException("Star-tree name cannot be null or empty");
            }
            if (groupingFields == null || groupingFields.isEmpty()) {
                throw new IllegalArgumentException("Star-tree [" + name + "] must have at least one grouping field");
            }
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException("Star-tree [" + name + "] must have at least one value");
            }
            if (maxLeafDocs <= 0) {
                throw new IllegalArgumentException("Star-tree [" + name + "] max_leaf_docs must be positive");
            }
            if (starNodeThreshold <= 0) {
                throw new IllegalArgumentException("Star-tree [" + name + "] star_node_threshold must be positive");
            }

            this.name = name;
            this.groupingFields = Collections.unmodifiableList(new ArrayList<>(groupingFields));
            this.values = Collections.unmodifiableList(new ArrayList<>(values));
            this.maxLeafDocs = maxLeafDocs;
            this.starNodeThreshold = starNodeThreshold;
        }

        public StarTreeFieldConfig(StreamInput in) throws IOException {
            this.name = in.readString();
            this.groupingFields = in.readCollectionAsImmutableList(StarTreeGroupingField::new);
            this.values = in.readCollectionAsImmutableList(StarTreeValue::new);
            this.maxLeafDocs = in.readVInt();
            this.starNodeThreshold = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeCollection(groupingFields);
            out.writeCollection(values);
            out.writeVInt(maxLeafDocs);
            out.writeVInt(starNodeThreshold);
        }

        public String getName() {
            return name;
        }

        public List<StarTreeGroupingField> getGroupingFields() {
            return groupingFields;
        }

        public List<StarTreeValue> getValues() {
            return values;
        }

        public int getMaxLeafDocs() {
            return maxLeafDocs;
        }

        public int getStarNodeThreshold() {
            return starNodeThreshold;
        }

        /**
         * Check if a field is a grouping field.
         */
        public boolean isGroupingField(String fieldName) {
            for (StarTreeGroupingField gf : groupingFields) {
                if (gf.getField().equals(fieldName)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Get a grouping field by field name.
         */
        public StarTreeGroupingField getGroupingField(String fieldName) {
            for (StarTreeGroupingField gf : groupingFields) {
                if (gf.getField().equals(fieldName)) {
                    return gf;
                }
            }
            return null;
        }

        /**
         * Check if a field is a value field.
         */
        public boolean isValue(String fieldName) {
            for (StarTreeValue value : values) {
                if (value.getField().equals(fieldName)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Get a value field by field name.
         */
        public StarTreeValue getValue(String fieldName) {
            for (StarTreeValue value : values) {
                if (value.getField().equals(fieldName)) {
                    return value;
                }
            }
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startArray(GROUPING_FIELDS_PARAM);
            for (StarTreeGroupingField gf : groupingFields) {
                gf.toXContent(builder, params);
            }
            builder.endArray();

            builder.startArray(VALUES_PARAM);
            for (StarTreeValue value : values) {
                value.toXContent(builder, params);
            }
            builder.endArray();

            boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
            if (maxLeafDocs != DEFAULT_MAX_LEAF_DOCS || starNodeThreshold != DEFAULT_STAR_NODE_THRESHOLD || includeDefaults) {
                builder.startObject(CONFIG_PARAM);
                if (maxLeafDocs != DEFAULT_MAX_LEAF_DOCS || includeDefaults) {
                    builder.field(MAX_LEAF_DOCS_PARAM, maxLeafDocs);
                }
                if (starNodeThreshold != DEFAULT_STAR_NODE_THRESHOLD || includeDefaults) {
                    builder.field(STAR_NODE_THRESHOLD_PARAM, starNodeThreshold);
                }
                builder.endObject();
            }

            builder.endObject();
            return builder;
        }

        /**
         * Parse a star-tree field config from XContent.
         */
        @SuppressWarnings("unchecked")
        public static StarTreeFieldConfig parse(Map<String, Object> configNode, String name) {
            List<StarTreeGroupingField> groupingFields = new ArrayList<>();
            List<StarTreeValue> values = new ArrayList<>();
            int maxLeafDocs = DEFAULT_MAX_LEAF_DOCS;
            int starNodeThreshold = DEFAULT_STAR_NODE_THRESHOLD;

            for (Map.Entry<String, Object> entry : configNode.entrySet()) {
                String paramName = entry.getKey();
                Object paramValue = entry.getValue();

                switch (paramName) {
                    case GROUPING_FIELDS_PARAM -> {
                        if (paramValue instanceof List == false) {
                            throw new MapperParsingException("Star-tree [" + name + "] grouping_fields must be an array");
                        }
                        for (Object gfNode : (List<?>) paramValue) {
                            groupingFields.add(StarTreeGroupingField.parse(gfNode));
                        }
                    }
                    case VALUES_PARAM -> {
                        if (paramValue instanceof List == false) {
                            throw new MapperParsingException("Star-tree [" + name + "] values must be an array");
                        }
                        for (Object valueNode : (List<?>) paramValue) {
                            values.add(StarTreeValue.parse(valueNode));
                        }
                    }
                    case CONFIG_PARAM -> {
                        if (paramValue instanceof Map == false) {
                            throw new MapperParsingException("Star-tree [" + name + "] config must be an object");
                        }
                        Map<String, Object> configMap = (Map<String, Object>) paramValue;
                        for (Map.Entry<String, Object> configEntry : configMap.entrySet()) {
                            String configKey = configEntry.getKey();
                            Object configValue = configEntry.getValue();
                            switch (configKey) {
                                case MAX_LEAF_DOCS_PARAM -> {
                                    if (configValue instanceof Number == false) {
                                        throw new MapperParsingException(
                                            "Star-tree [" + name + "] " + MAX_LEAF_DOCS_PARAM + " must be a number"
                                        );
                                    }
                                    maxLeafDocs = ((Number) configValue).intValue();
                                }
                                case STAR_NODE_THRESHOLD_PARAM -> {
                                    if (configValue instanceof Number == false) {
                                        throw new MapperParsingException(
                                            "Star-tree [" + name + "] " + STAR_NODE_THRESHOLD_PARAM + " must be a number"
                                        );
                                    }
                                    starNodeThreshold = ((Number) configValue).intValue();
                                }
                                default -> throw new MapperParsingException(
                                    "Unknown star-tree config parameter [" + configKey + "]"
                                );
                            }
                        }
                    }
                    default -> throw new MapperParsingException(
                        "Unknown star-tree [" + name + "] parameter [" + paramName + "]"
                    );
                }
            }

            return new StarTreeFieldConfig(name, groupingFields, values, maxLeafDocs, starNodeThreshold);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StarTreeFieldConfig that = (StarTreeFieldConfig) o;
            return maxLeafDocs == that.maxLeafDocs
                && starNodeThreshold == that.starNodeThreshold
                && Objects.equals(name, that.name)
                && Objects.equals(groupingFields, that.groupingFields)
                && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, groupingFields, values, maxLeafDocs, starNodeThreshold);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
