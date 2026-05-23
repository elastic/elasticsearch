/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.tree;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Maps identifier-like strings — column / index names, literal values — to the form
 * {@code nodeString} should emit. The {@link #IDENTITY} mapper passes everything through
 * unchanged; the failure-path anonymizer substitutes interned tokens.
 * <p>
 * Passed as a separate argument to {@code Node.nodeString} alongside a
 * {@link Node.NodeStringFormat}: format controls output shape (truncation, width limits);
 * mapper controls identifier emission. Each {@code nodeString} implementation that
 * mentions an identifier or literal asks the mapper for the value to emit.
 * <p>
 * Stays narrow on purpose: only three methods, all generic. Pattern-bearing classes
 * (Dissect, Grok, RegexMatch, UnresolvedNamePattern, ...) parse their own pattern
 * structure and route each extracted capture / identifier through {@link #column}; the
 * mapper never carries syntax-specific knowledge. Adding a new pattern type doesn't touch
 * this interface.
 */
public interface IdentifierMapper {

    /** Pass-through. The default for raw rendering. */
    IdentifierMapper IDENTITY = new IdentifierMapper() {};

    /** Map a column / attribute / alias / qualifier / pattern capture name. */
    default String column(String name) {
        return name;
    }

    /** Map an index pattern / concrete index / view / enrich-policy index name. */
    default String index(String name) {
        return name;
    }

    /**
     * Map a literal value of the given data type. Returns just the value portion;
     * {@code "[type]"} suffix is appended by the caller so {@code "5[LONG]"} and
     * {@code "0[LONG]"} share the same shape. Default renders the value's natural string
     * form (decoding {@link BytesRef} as UTF-8).
     */
    default String literal(Object value, DataType type) {
        if (value == null) {
            return "null";
        }
        return value instanceof BytesRef br ? br.utf8ToString() : String.valueOf(value);
    }
}
