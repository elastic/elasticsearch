/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.tree;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.versionfield.Version;

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

    /**
     * Map a free-form / opaque text fragment that carries no parseable identifier structure — a raw
     * Lucene {@code QueryBuilder} DSL, a sort or stats descriptor, an external source path. Returns
     * the text verbatim under {@link #IDENTITY}; an anonymizing mapper returns a redaction marker,
     * since such text can't be safely tokenized without risking a partial leak. Lets a node keep the
     * field in place with no {@code == IDENTITY} branch — same shape, redacted value.
     */
    String opaque(String text);

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
            // Canonical raw rendering of a literal value (without the [type] suffix or LIMITED
            // truncation, which callers apply). Only KEYWORD/TEXT/VERSION are UTF-8-backed BytesRefs;
            // spatial types (geo_point/geo_shape/cartesian_*) carry WKB binary, so utf8ToString would
            // garble or throw — they fall through to String.valueOf (a safe hex rendering).
            if (value == null) {
                return "null";
            }
            if (type == DataType.KEYWORD || type == DataType.TEXT) {
                return BytesRefs.toString(value);
            }
            if (type == DataType.VERSION && value instanceof BytesRef br) {
                return new Version(br).toString();
            }
            return String.valueOf(value);
        }

        @Override
        public String opaque(String text) {
            return text;
        }
    };
}
