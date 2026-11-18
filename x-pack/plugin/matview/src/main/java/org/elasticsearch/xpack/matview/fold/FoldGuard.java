/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.util.Bits;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Decides which documents of a merge input may take part in folding. Documents outside the returned bits are
 * neither hidden, nor chosen as representatives, nor fed to the {@link Reducer}: they keep their identity and
 * values. The engine uses this to keep documents that are still part of the retained operation history intact,
 * since recovery and replication read them back by sequence number.
 */
@FunctionalInterface
public interface FoldGuard {
    /** Documents allowed to fold, or {@code null} when every live document of the reader may fold. */
    @Nullable
    Bits foldable(CodecReader reader) throws IOException;
}
