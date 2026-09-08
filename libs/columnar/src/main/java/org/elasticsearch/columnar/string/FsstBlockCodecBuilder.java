/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/**
 * Builds a {@link FsstBlockCodec} with tunable parameters. All settings default to the values that
 * produce standard FSST behaviour; only override when tuning is needed.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * FsstBlockCodec codec = new FsstBlockCodecBuilder().build();
 * FsstBlockCodec codec = new FsstBlockCodecBuilder().maxSymbolLength(6).build();
 * }</pre>
 */
final class FsstBlockCodecBuilder {

    /**
     * Default maximum symbol length. Capped at 4 rather than {@link FsstSymbolTable#MAX_SYMBOL_LENGTH}
     * because each extra extension step is an O(block size) scan; 4-byte symbols cover the most
     * frequent patterns in structured text (URL prefixes, log tokens) while keeping training cost
     * proportional to two extension steps per candidate instead of six.
     */
    private static final int DEFAULT_MAX_SYMBOL_LENGTH = 4;

    private int maxSymbolLength = DEFAULT_MAX_SYMBOL_LENGTH;

    FsstBlockCodecBuilder() {}

    /**
     * Maximum byte length of a symbol the symbol-table builder may produce. Longer symbols compress
     * structured strings more aggressively at the cost of a more expensive training scan; shorter
     * symbols reduce training cost but leave more bytes uncovered per block. Must be between 1 and
     * {@link FsstSymbolTable#MAX_SYMBOL_LENGTH} inclusive.
     */
    FsstBlockCodecBuilder maxSymbolLength(int maxSymbolLength) {
        if (maxSymbolLength < 1 || maxSymbolLength > FsstSymbolTable.MAX_SYMBOL_LENGTH) {
            throw new IllegalArgumentException(
                "maxSymbolLength must be between 1 and " + FsstSymbolTable.MAX_SYMBOL_LENGTH + ", got " + maxSymbolLength
            );
        }
        this.maxSymbolLength = maxSymbolLength;
        return this;
    }

    /** Returns a {@link FsstBlockCodec} configured with the parameters set on this builder. */
    FsstBlockCodec build() {
        return FsstBlockCodec.of(maxSymbolLength);
    }
}
