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

/**
 * Marks a {@link Directory} that only knows about the files created through it, so files must be copied into it rather than
 * hard-linked into the file system directory it wraps.
 */
public interface NoHardLinksDirectory {

    /**
     * Returns {@code true} if the given directory or any directory it wraps is a
     * {@link NoHardLinksDirectory}.
     */
    static boolean hasNoHardLinksDirectory(Directory directory) {
        while (true) {
            if (directory instanceof NoHardLinksDirectory) {
                return true;
            }
            if (directory instanceof FilterDirectory filterDirectory) {
                // Walk one level at a time: FilterDirectory.unwrap would skip any marked directories in the middle of the chain.
                directory = filterDirectory.getDelegate();
            } else {
                return false;
            }
        }
    }
}
