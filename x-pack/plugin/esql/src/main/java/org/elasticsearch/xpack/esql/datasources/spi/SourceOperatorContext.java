/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.ExternalSliceQueue;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    int rowLimit,
    Executor executor,
    @Nullable Executor fileReadExecutor,
    Map<String, Object> config,
    Map<String, Object> sourceMetadata,
    Object pushedFilter,
    List<Expression> pushedExpressions,
    FileList fileList,
    @Nullable ExternalSplit split,
    Set<String> partitionColumnNames,
    @Nullable ExternalSliceQueue sliceQueue,
    int parsingParallelism,
    int parallelism
) {
    public SourceOperatorContext {
        Check.notNull(path, "path cannot be null");
        Check.notNull(executor, "executor cannot be null");
        projectedColumns = projectedColumns != null ? List.copyOf(projectedColumns) : List.of();
        attributes = attributes != null ? List.copyOf(attributes) : List.of();
        config = config != null ? Map.copyOf(config) : Map.of();
        sourceMetadata = sourceMetadata != null ? Map.copyOf(sourceMetadata) : Map.of();
        pushedExpressions = pushedExpressions != null ? List.copyOf(pushedExpressions) : List.of();
        partitionColumnNames = partitionColumnNames != null && partitionColumnNames.isEmpty() == false
            ? Collections.unmodifiableSet(new LinkedHashSet<>(partitionColumnNames))
            : Set.of();

        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive, got: " + maxBufferSize);
        }
        if (parsingParallelism < 1) {
            throw new IllegalArgumentException("parsingParallelism must be >= 1, got: " + parsingParallelism);
        }
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1, got: " + parallelism);
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
        Object pushedFilter,
        FileList fileList,
        @Nullable ExternalSplit split
    ) {
        this(
            sourceType,
            path,
            projectedColumns,
            attributes,
            batchSize,
            maxBufferSize,
            FormatReader.NO_LIMIT,
            executor,
            null,
            config,
            sourceMetadata,
            pushedFilter,
            null,
            fileList,
            split,
            null,
            null,
            1,
            1
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
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        FileList fileList
    ) {
        this(
            sourceType,
            path,
            projectedColumns,
            attributes,
            batchSize,
            maxBufferSize,
            FormatReader.NO_LIMIT,
            executor,
            null,
            config,
            sourceMetadata,
            pushedFilter,
            null,
            fileList,
            null,
            null,
            null,
            1,
            1
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
            FormatReader.NO_LIMIT,
            executor,
            null,
            config,
            sourceMetadata,
            pushedFilter,
            null,
            null,
            null,
            null,
            null,
            1,
            1
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
        this(
            sourceType,
            path,
            projectedColumns,
            attributes,
            batchSize,
            maxBufferSize,
            FormatReader.NO_LIMIT,
            executor,
            null,
            config,
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            1,
            1
        );
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
        private int rowLimit = FormatReader.NO_LIMIT;
        private Executor executor;
        @Nullable
        private Executor fileReadExecutor;
        private Map<String, Object> config;
        private Map<String, Object> sourceMetadata;
        private Object pushedFilter;
        private List<Expression> pushedExpressions;
        private FileList fileList;
        private ExternalSplit split;
        private Set<String> partitionColumnNames;
        private ExternalSliceQueue sliceQueue;
        private int parsingParallelism = 1;
        private int parallelism = 1;

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

        public Builder rowLimit(int rowLimit) {
            this.rowLimit = rowLimit;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Optional executor for file (and similar) background reads. When set (e.g. to {@code generic}),
         * async reads and slice-queue drain run here instead of on {@link #executor}, so producers blocked
         * in buffer backpressure do not starve the {@code esql_worker} drivers that consume the buffer.
         */
        public Builder fileReadExecutor(Executor fileReadExecutor) {
            this.fileReadExecutor = fileReadExecutor;
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

        public Builder pushedExpressions(List<Expression> pushedExpressions) {
            this.pushedExpressions = pushedExpressions;
            return this;
        }

        public Builder fileList(FileList fileList) {
            this.fileList = fileList;
            return this;
        }

        public Builder split(ExternalSplit split) {
            this.split = split;
            return this;
        }

        public Builder partitionColumnNames(Set<String> partitionColumnNames) {
            this.partitionColumnNames = partitionColumnNames;
            return this;
        }

        public Builder sliceQueue(ExternalSliceQueue sliceQueue) {
            this.sliceQueue = sliceQueue;
            return this;
        }

        public Builder parsingParallelism(int parsingParallelism) {
            this.parsingParallelism = parsingParallelism;
            return this;
        }

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
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
                rowLimit,
                executor,
                fileReadExecutor,
                config,
                sourceMetadata,
                pushedFilter,
                pushedExpressions,
                fileList,
                split,
                partitionColumnNames,
                sliceQueue,
                parsingParallelism,
                parallelism
            );
        }
    }
}
