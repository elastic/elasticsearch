/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.FileSet;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Context for creating source operator factories.
 * Uses Java record for immutability and automatic equals/hashCode/toString.
 *
 * <p>Note: Record accessors have no "get" prefix per project conventions.
 *
 * <p>This context is passed to {@link SourceOperatorFactoryProvider} implementations
 * to provide all necessary information for creating a source operator factory.
 *
 * <p>For table-based sources (Iceberg, Delta Lake), the {@link #sourceMetadata()} map
 * contains opaque source-specific data (like native schema) that the operator factory
 * needs but core doesn't interpret.
 *
 * <p>The {@link #pushedFilter()} contains an opaque filter object that was pushed down
 * during optimization. Since external sources execute on coordinator only, this filter
 * is never serialized - it's created during local physical optimization and consumed
 * immediately by the operator factory in the same JVM.
 */
public record SourceOperatorContext(
    String sourceType,
    StoragePath path,
    List<String> projectedColumns,
    List<Attribute> attributes,
    int batchSize,
    int maxBufferSize,
    Executor executor,
    Map<String, Object> config,
    Map<String, Object> sourceMetadata,
    Object pushedFilter,
    FileSet fileSet
) {
    public SourceOperatorContext {
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        projectedColumns = projectedColumns != null ? List.copyOf(projectedColumns) : List.of();
        attributes = attributes != null ? List.copyOf(attributes) : List.of();
        config = config != null ? Map.copyOf(config) : Map.of();
        sourceMetadata = sourceMetadata != null ? Map.copyOf(sourceMetadata) : Map.of();

        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive, got: " + maxBufferSize);
        }
    }

    public SourceOperatorContext(
        String sourceType,
        StoragePath path,
        List<String> projectedColumns,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter
    ) {
        this(
            sourceType,
            path,
            projectedColumns,
            attributes,
            batchSize,
            maxBufferSize,
            executor,
            config,
            sourceMetadata,
            pushedFilter,
            null
        );
    }

    public SourceOperatorContext(
        String sourceType,
        StoragePath path,
        List<String> projectedColumns,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor,
        Map<String, Object> config
    ) {
        this(sourceType, path, projectedColumns, attributes, batchSize, maxBufferSize, executor, config, Map.of(), null, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String sourceType;
        private StoragePath path;
        private List<String> projectedColumns;
        private List<Attribute> attributes;
        private int batchSize = 1000;
        private int maxBufferSize = 10;
        private Executor executor;
        private Map<String, Object> config;
        private Map<String, Object> sourceMetadata;
        private Object pushedFilter;
        private FileSet fileSet;

        public Builder sourceType(String sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        public Builder path(StoragePath path) {
            this.path = path;
            return this;
        }

        public Builder projectedColumns(List<String> projectedColumns) {
            this.projectedColumns = projectedColumns;
            return this;
        }

        public Builder attributes(List<Attribute> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder maxBufferSize(int maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Builder config(Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public Builder sourceMetadata(Map<String, Object> sourceMetadata) {
            this.sourceMetadata = sourceMetadata;
            return this;
        }

        public Builder pushedFilter(Object pushedFilter) {
            this.pushedFilter = pushedFilter;
            return this;
        }

        public Builder fileSet(FileSet fileSet) {
            this.fileSet = fileSet;
            return this;
        }

        public SourceOperatorContext build() {
            return new SourceOperatorContext(
                sourceType,
                path,
                projectedColumns,
                attributes,
                batchSize,
                maxBufferSize,
                executor,
                config,
                sourceMetadata,
                pushedFilter,
                fileSet
            );
        }
    }
}
