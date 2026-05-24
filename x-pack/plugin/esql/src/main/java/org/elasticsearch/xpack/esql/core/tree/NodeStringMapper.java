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
 * Identifier-substitution strategy used during {@code Node.nodeString} rendering. Pairs with
 * {@link Node.NodeStringFormat}: format controls output shape (truncation, width); this mapper
 * controls how column / index names and literal values are emitted. Each {@code nodeString}
 * implementation that mentions an identifier or literal asks the mapper for the value to emit.
 * <p>
 * All methods are abstract on purpose: every implementer must make a deliberate decision per
 * identifier kind. A {@code default} method here would let a partial override silently inherit
 * pass-through behavior for the methods it didn't define, which is exactly the leak vector this
 * SPI exists to prevent. The {@link #IDENTITY} constant is one concrete implementation of the
 * contract — pass-through for all three kinds — embedded here so call sites that want the raw
 * rendering can name it directly.
 * <p>
 * Stays narrow on purpose: only three methods, all generic. Pattern-bearing classes (Dissect,
 * Grok, RegexMatch, UnresolvedNamePattern, ...) parse their own pattern structure and route each
 * extracted capture / identifier through {@link #column}; the mapper never carries syntax-
 * specific knowledge. Adding a new pattern type doesn't touch this interface.
 */
public interface NodeStringMapper {

    /** Map a column / attribute / alias / qualifier / pattern capture name. */
    String column(String name);

    /** Map an index pattern / concrete index / view / enrich-policy index name. */
    String index(String name);

    /**
     * Map a literal value of the given data type. Returns just the value portion;
     * {@code "[type]"} suffix is appended by the caller so {@code "5[LONG]"} and
     * {@code "0[LONG]"} share the same shape.
     */
    String literal(Object value, DataType type);

    /** Pass-through. The default for raw rendering. */
    NodeStringMapper IDENTITY = new NodeStringMapper() {
        @Override
        public String column(String name) {
            return name;
        }

        @Override
        public String index(String name) {
            return name;
        }

        @Override
        public String literal(Object value, DataType type) {
            if (value == null) {
                return "null";
            }
            return value instanceof BytesRef br ? br.utf8ToString() : String.valueOf(value);
        }
    };
}
