/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class StoreMetricsDirectory extends ByteSizeDirectory {
    private final PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder;

    public StoreMetricsDirectory(ByteSizeDirectory in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        super(in);
        this.metricHolder = metricHolder;
        assert in instanceof StoreMetricsDirectory == false;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return StoreMetricsIndexInput.create(name, super.openInput(name, context), metricHolder.singleThreaded());
    }

    @Override
    public long estimateSizeInBytes() throws IOException {
        return ((ByteSizeDirectory) getDelegate()).estimateSizeInBytes();
    }

    @Override
    public long estimateDataSetSizeInBytes() throws IOException {
        return ((ByteSizeDirectory) getDelegate()).estimateDataSetSizeInBytes();
    }
}
