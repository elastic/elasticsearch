/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;

/**
 * Per-segment mutable scratch buffers shared by the per-mode codecs. Held by
 * {@link SortedOrdinalCodec} and threaded through the encode/decode
 * methods so the codec classes themselves stay stateless and may be exposed
 * as singletons.
 */
final class CodecContext {

    final DocValuesForUtil forUtil;
    final long[] scratch;

    CodecContext(int blockSize) {
        this.forUtil = new DocValuesForUtil(blockSize);
        this.scratch = new long[blockSize];
    }
}
