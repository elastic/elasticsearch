/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;

/**
 * The outcome of parsing a single field value.
 * {@link FallbackPostMapper} switches exhaustively on this to route to exactly one fallback destination.
 */
public sealed interface ParseResult permits ParseResult.Indexed, ParseResult.Ignored, ParseResult.MultiValueViolation {

    /** The value was parsed and indexed successfully; no fallback write is needed. */
    record Indexed() implements ParseResult {}

    /**
     * The field was ignored during parsing (e.g. {@code ignore_malformed} or {@code ignore_above});
     * the mapper wrote to its own fallback destination.
     */
    record Ignored() implements ParseResult {}

    /**
     * A {@code multi_value=false} constraint was violated. {@code capturedValue} holds the encoded
     * violating token for storage in {@code ._on_failure}.
     */
    record MultiValueViolation(BytesRef capturedValue) implements ParseResult {}

    /** Singleton for the common indexed result; avoids repeated allocation of a zero-field record. */
    Indexed INDEXED = new Indexed();
    /** Singleton for the common ignored result; avoids repeated allocation of a zero-field record. */
    Ignored IGNORED = new Ignored();
}
