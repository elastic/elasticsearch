/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

/**
 * Reusable scratch buffers for {@link PageColumnReader} batch decoding. Provides a scratch
 * {@code int[]} for type-widening conversions, plus {@link WordMask} instances for null and
 * value-selection bit masks.
 *
 * <p>Buffers are pure scratch space: {@link PageColumnReader} decodes into them, then copies the
 * decoded slice into a freshly allocated, tightly sized array before handing it to
 * {@code BlockFactory}. The Block therefore never aliases anything inside this class, so the
 * reader is free to overwrite the scratch arrays on the next batch. See the {@code Block
 * construction helpers} section in {@link PageColumnReader} for the full ownership rationale.
 *
 * <p>Each buffer is lazily allocated and grown as needed.
 *
 * <p>Each {@link PageColumnReader} owns its own instance. Sharing across columns is unnecessary
 * and would only complicate ownership without buying anything.
 */
final class DecodeBuffers {

    private int[] intBuf;

    DecodeBuffers() {}

    int[] ints(int minSize) {
        if (intBuf == null || intBuf.length < minSize) {
            intBuf = new int[minSize];
        }
        return intBuf;
    }

    private WordMask nullsMask;
    private WordMask valueSelMask;

    WordMask nullsMask(int numBits) {
        if (nullsMask == null) nullsMask = new WordMask();
        nullsMask.reset(numBits);
        return nullsMask;
    }

    WordMask valueSelection(int numBits) {
        if (valueSelMask == null) valueSelMask = new WordMask();
        valueSelMask.reset(numBits);
        return valueSelMask;
    }
}
