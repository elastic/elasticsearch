/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Simple immutable implementation of SourceMetadata.
 * Suitable for use by FormatReader implementations and as a base for
 * table-based sources that need to pass through opaque metadata.
 */
public final class SimpleSourceMetadata implements SourceMetadata {

    private final List<Attribute> schema;
    private final String sourceType;
    private final String location;
    private final SourceStatistics statistics;
    private final List<String> partitionColumns;
    private final Map<String, Object> sourceMetadata;
    private final Map<String, Object> config;

    /**
     * Creates a SimpleSourceMetadata with required fields only.
     */
    public SimpleSourceMetadata(List<Attribute> schema, String sourceType, String location) {
        this(schema, sourceType, location, null, null, null, null);
    }

    /**
     * Creates a SimpleSourceMetadata with statistics and partition columns.
     */
    public SimpleSourceMetadata(
        List<Attribute> schema,
        String sourceType,
        String location,
        SourceStatistics statistics,
        List<String> partitionColumns
    ) {
        this(schema, sourceType, location, statistics, partitionColumns, null, null);
    }

    /**
     * Creates a SimpleSourceMetadata with all fields including opaque metadata and config.
     */
    public SimpleSourceMetadata(
        List<Attribute> schema,
        String sourceType,
        String location,
        SourceStatistics statistics,
        List<String> partitionColumns,
        Map<String, Object> sourceMetadata,
        Map<String, Object> config
    ) {
        if (schema == null) {
            throw new IllegalArgumentException("schema must not be null");
        }
        if (sourceType == null) {
            throw new IllegalArgumentException("sourceType must not be null");
        }
        if (location == null) {
            throw new IllegalArgumentException("location must not be null");
        }
        this.schema = schema;
        this.sourceType = sourceType;
        this.location = location;
        this.statistics = statistics;
        this.partitionColumns = partitionColumns;
        this.sourceMetadata = sourceMetadata != null ? Map.copyOf(sourceMetadata) : Map.of();
        this.config = config != null ? Map.copyOf(config) : Map.of();
    }

    @Override
    public List<Attribute> schema() {
        return schema;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public Optional<SourceStatistics> statistics() {
        return Optional.ofNullable(statistics);
    }

    @Override
    public Optional<List<String>> partitionColumns() {
        return Optional.ofNullable(partitionColumns);
    }

    @Override
    public Map<String, Object> sourceMetadata() {
        return sourceMetadata;
    }

    @Override
    public Map<String, Object> config() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleSourceMetadata that = (SimpleSourceMetadata) o;
        boolean schemaEquals = schema == null ? that.schema == null : schema.equals(that.schema);
        boolean sourceTypeEquals = sourceType == null ? that.sourceType == null : sourceType.equals(that.sourceType);
        boolean locationEquals = location == null ? that.location == null : location.equals(that.location);
        return schemaEquals && sourceTypeEquals && locationEquals;
    }

    @Override
    public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + (sourceType != null ? sourceType.hashCode() : 0);
        result = 31 * result + (location != null ? location.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SimpleSourceMetadata{sourceType='" + sourceType + "', location='" + location + "', fields=" + schema.size() + "}";
    }

    /**
     * Creates a builder for constructing SimpleSourceMetadata instances.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for SimpleSourceMetadata.
     */
    public static class Builder {
        private List<Attribute> schema;
        private String sourceType;
        private String location;
        private SourceStatistics statistics;
        private List<String> partitionColumns;
        private Map<String, Object> sourceMetadata;
        private Map<String, Object> config;

        public Builder schema(List<Attribute> schema) {
            this.schema = schema;
            return this;
        }

        public Builder sourceType(String sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        public Builder location(String location) {
            this.location = location;
            return this;
        }

        public Builder statistics(SourceStatistics statistics) {
            this.statistics = statistics;
            return this;
        }

        public Builder partitionColumns(List<String> partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder sourceMetadata(Map<String, Object> sourceMetadata) {
            this.sourceMetadata = sourceMetadata;
            return this;
        }

        public Builder config(Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public SimpleSourceMetadata build() {
            return new SimpleSourceMetadata(schema, sourceType, location, statistics, partitionColumns, sourceMetadata, config);
        }
    }
}
