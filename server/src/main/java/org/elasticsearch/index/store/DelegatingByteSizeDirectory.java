/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * A {@link ByteSizeDirectory} whose size estimates are those of its delegate if it is a {@link ByteSizeDirectory}, and
 * otherwise a walk of the delegate's files. Subclasses add a layer to a directory chain without changing its size, and
 * {@link #unwrap} finds such a layer in a {@link FilterDirectory} chain.
 */
abstract class DelegatingByteSizeDirectory extends ByteSizeDirectory {

    DelegatingByteSizeDirectory(Directory delegate) {
        super(delegate);
    }

    /**
     * Walks the {@link FilterDirectory} chain and returns the first directory of the given type, or {@code null} if none is
     * present.
     */
    @Nullable
    static <T extends Directory> T unwrap(Directory directory, Class<T> type) {
        Directory d = directory;
        while (d != null) {
            if (type.isInstance(d)) {
                return type.cast(d);
            }
            if (d instanceof FilterDirectory fd) {
                d = fd.getDelegate();
            } else {
                return null;
            }
        }
        return null;
    }

    @Override
    public final long estimateSizeInBytes() throws IOException {
        Directory delegate = getDelegate();
        if (delegate instanceof ByteSizeDirectory bsd) {
            return bsd.estimateSizeInBytes();
        }
        return estimateSizeInBytes(delegate);
    }

    @Override
    public final long estimateDataSetSizeInBytes() throws IOException {
        Directory delegate = getDelegate();
        if (delegate instanceof ByteSizeDirectory bsd) {
            return bsd.estimateDataSetSizeInBytes();
        }
        return estimateSizeInBytes(delegate);
    }
}
