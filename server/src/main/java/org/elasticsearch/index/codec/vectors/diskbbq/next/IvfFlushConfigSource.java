/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.Optional;

/**
 * Optional per-field configuration loaded on flush before building IVF structures.
 * Default to {@link #empty()}.
 */
@FunctionalInterface
public interface IvfFlushConfigSource {

    Optional<IvfSegmentConfig> load(SegmentWriteState state, FieldInfo fieldInfo) throws IOException;

    static IvfFlushConfigSource empty() {
        return (state, fieldInfo) -> Optional.empty();
    }
}
