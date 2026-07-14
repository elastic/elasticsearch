/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * An ESCF column whose values are all raw binary bytes (variable-length layout: offset vector + dense byte
 * payload).
 */
final class EscfBinaryColumn extends AbstractVarColumn {

    EscfBinaryColumn(int docCount, FixedBitSet absent, BytesReference data, int[] offsets) {
        super(docCount, absent, data, offsets);
    }

    @Override
    byte kind() {
        return EscfColumnKind.BINARY;
    }

    @Override
    byte typeByteForPresent(int d) {
        return SourceValueType.BINARY;
    }
}
