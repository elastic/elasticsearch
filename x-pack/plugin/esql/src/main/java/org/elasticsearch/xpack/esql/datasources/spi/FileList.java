/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;

/**
 * Indexed view over a set of files. Implemented by {@link org.elasticsearch.xpack.esql.datasources.GenericFileList} (full objects),
 * {@link org.elasticsearch.xpack.esql.datasources.cache.DictionaryFileList} (segment dictionary), and
 * {@link org.elasticsearch.xpack.esql.datasources.cache.HiveFileList} (partition-grouped).
 * Downstream consumers (FileSplitProvider, etc.) use this interface instead of
 * accessing GenericFileList directly, enabling compact cached representations.
 */
public interface FileList {
    int fileCount();

    StoragePath path(int i);

    long size(int i);

    long lastModifiedMillis(int i);

    @Nullable
    String originalPattern();

    @Nullable
    PartitionMetadata partitionMetadata();

    boolean isResolved();

    boolean isEmpty();

    long estimatedBytes();
}
