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
import org.elasticsearch.sourcebatch.SourceValueType;

/** An ESCF column whose values are all {@code long}s (JSON ints and longs upcast to 64-bit). */
final class ElasticsearchLongColumn extends AbstractFixed64Column {

    ElasticsearchLongColumn(int columnIndex, int docCount, FixedBitSet absent, byte[] data, int base) {
        super(columnIndex, docCount, absent, data, base);
    }

    @Override
    byte kind() {
        return ElasticsearchColumnKind.LONG;
    }

    @Override
    byte typeByteForPresent(int d) {
        return SourceValueType.LONG;
    }

    @Override
    long getLongValue(int d) {
        return rawLong(d);
    }
}
