/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;

public interface MemorySegmentAccessInputAccess {
    MemorySegmentAccessInput get();

    static IndexInput unwrap(IndexInput input) {
        return input instanceof MemorySegmentAccessInputAccess msaia ? (IndexInput) msaia.get() : input;
    }
}
