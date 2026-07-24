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
 * {@link FallbackStorageRouter} switches exhaustively on this to route to exactly one fallback destination.
 */
public sealed interface ParseResult permits ParseResult.Indexed, ParseResult.Malformed, ParseResult.MultiValueViolation {

    /** The value was parsed and indexed successfully; no fallback write is needed. */
    record Indexed() implements ParseResult {}

    /** Malformed with {@code ignore_malformed} enabled; the mapper already wrote to {@code ._ignore_malformed}. */
    record Malformed() implements ParseResult {}

    /**
     * A {@code multi_value=false} constraint was violated. {@code capturedValue} holds the encoded
     * violating token for storage in {@code ._on_failure}.
     */
    record MultiValueViolation(BytesRef capturedValue) implements ParseResult {}
}
