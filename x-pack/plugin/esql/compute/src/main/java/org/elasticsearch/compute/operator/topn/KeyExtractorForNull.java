/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRefBuilder;

class KeyExtractorForNull implements KeyExtractor {
    private final byte nul;

    KeyExtractorForNull(byte nul) {
        this.nul = nul;
    }

    @Override
    public int writeKey(BytesRefBuilder values, int position) {
        values.append(nul);
        return 1;
    }

    @Override
    public String toString() {
        return "KeyExtractorForNull(" + Integer.toHexString(nul & 0xff) + ")";
    }
}
