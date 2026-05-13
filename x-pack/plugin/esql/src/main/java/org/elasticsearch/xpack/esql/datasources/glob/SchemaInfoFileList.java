/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.Map;

/**
 * Decorating {@link FileList} that overlays {@link #fileSchemaInfo()} onto an inner list.
 * Lets the resolver attach per-file schema info to any FileList implementation
 * (e.g. {@code DictionaryFileList}, {@code HiveFileList}) without each implementation
 * carrying its own schema-info field.
 */
final class SchemaInfoFileList implements FileList {

    private final FileList delegate;
    private final Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo;

    SchemaInfoFileList(FileList delegate, Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo) {
        this.delegate = delegate;
        this.schemaInfo = schemaInfo;
    }

    @Override
    public int fileCount() {
        return delegate.fileCount();
    }

    @Override
    public StoragePath path(int i) {
        return delegate.path(i);
    }

    @Override
    public long size(int i) {
        return delegate.size(i);
    }

    @Override
    public long lastModifiedMillis(int i) {
        return delegate.lastModifiedMillis(i);
    }

    @Override
    @Nullable
    public String originalPattern() {
        return delegate.originalPattern();
    }

    @Override
    @Nullable
    public PartitionMetadata partitionMetadata() {
        return delegate.partitionMetadata();
    }

    @Override
    public boolean isResolved() {
        return delegate.isResolved();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public long estimatedBytes() {
        return delegate.estimatedBytes();
    }

    @Override
    @Nullable
    public Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo() {
        return schemaInfo;
    }
}
