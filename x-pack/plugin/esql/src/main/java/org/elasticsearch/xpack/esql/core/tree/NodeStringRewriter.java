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
 * Plug-in for the {@link Node#nodeString(StringBuilder, Node.NodeStringFormat) nodeString} render
 * pipeline. Maps identifier-like strings — column / index names, literal values — to their
 * rendered form. The {@link #IDENTITY} implementation passes everything through unchanged; other
 * implementations (e.g. the failure-path anonymizer) substitute tokens.
 * <p>
 * Carried inside {@link Node.NodeStringFormat} so a single render pass through a plan tree can be
 * raw, anonymizing, or any other identifier-rewriting variant. Each {@code nodeString}
 * implementation that mentions an identifier or literal asks the rewriter to map it, rather than
 * branching on the format type.
 * <p>
 * Stays narrow on purpose: only three methods, all generic. Pattern-bearing classes (Dissect,
 * Grok, RegexMatch, UnresolvedNamePattern, ...) parse their own pattern structure and route each
 * extracted capture / identifier through {@link #column}; the rewriter never carries syntax-
 * specific knowledge. Adding a new pattern type doesn't touch this interface.
 */
public interface NodeStringRewriter {

    /** Pass-through. Used by {@code LIMITED} and {@code FULL} formats. */
    NodeStringRewriter IDENTITY = new NodeStringRewriter() {};

    /** Tokenize a column / attribute / alias / qualifier / pattern capture name. */
    default String column(String name) {
        return name;
    }

    /** Tokenize an index pattern / concrete index / view / enrich-policy index name. */
    default String index(String name) {
        return name;
    }

    /**
     * Tokenize a literal value of the given data type. Returns just the value portion;
     * {@code "[type]"} suffix is appended by the caller so {@code "5[LONG]"} and
     * {@code "0[LONG]"} share the same shape. Default renders the value's natural string form
     * (decoding {@link BytesRef} as UTF-8).
     */
    default String literal(Object value, DataType type) {
        if (value == null) {
            return "null";
        }
        return value instanceof BytesRef br ? br.utf8ToString() : String.valueOf(value);
    }
}
