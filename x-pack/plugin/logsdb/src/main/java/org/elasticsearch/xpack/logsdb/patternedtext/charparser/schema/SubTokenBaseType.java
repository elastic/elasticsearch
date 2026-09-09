/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

public record SubTokenBaseType(
    String name,
    EncodingType encodingType,
    String symbol,
    Class<?> baseType,
    String description,
    char[] allowedCharacters
) {

    /**
     * Whether this base type admits negative values. Signed integers ({@code %J}) do; unsigned integers ({@code %I}) and everything else
     * do not. Used by the compiler to floor the value range of unsigned numeric subTokens at 0 (both {@code %I} and {@code %J} share the
     * {@link EncodingType#INTEGER} encoding, so the symbol is the only discriminator).
     */
    public boolean isSigned() {
        return "J".equals(symbol);
    }

    @Override
    public String toString() {
        return name;
    }
}
