/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

class KeyExtractorForNull implements KeyExtractor {
    private final byte nul;

    KeyExtractorForNull(byte nul) {
        this.nul = nul;
    }

    @Override
    public void writeKey(BreakingBytesRefBuilder values, int position) {
        values.append(nul);
    }

    @Override
    public String toString() {
        return "KeyExtractorForNull(" + Integer.toHexString(nul & 0xff) + ")";
    }
}
