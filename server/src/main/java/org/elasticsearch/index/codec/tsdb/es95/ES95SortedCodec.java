/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.DefaultSortedCodec;

/**
 * {@link org.elasticsearch.index.codec.tsdb.SortedOrdinalCodec} for the ES95 TSDB format. Encodes the
 * sorted ordinal stream with {@link ES95OrdinalCodec}, producing bytes identical to the ordinal
 * stream ES95 wrote before the sorted/sorted-set split. A run-table codec composes this as its
 * fallback for fields it does not encode itself.
 */
final class ES95SortedCodec extends DefaultSortedCodec {

    ES95SortedCodec() {
        super(new ES95OrdinalCodec());
    }
}
