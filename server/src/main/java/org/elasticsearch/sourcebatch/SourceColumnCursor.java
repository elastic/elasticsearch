/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.xcontent.Text;

/**
 * A forward, single-pass view over a {@link SourceColumn} that visits <b>every</b> document in order
 * and exposes its {@link EirfType} byte together with the matching value. Unlike the random-access
 * {@link SourceColumn} getters — which re-resolve per-document state on each call and dispatch
 * polymorphically across every column implementation — a cursor decodes each document once from
 * already-advanced state, which suits the columnar transform loops that rebuild one column into
 * another document-by-document.
 *
 * <p>Usage: {@code while (cursor.advance()) { switch (cursor.type()) { ... } }}. Each value accessor is
 * valid only when {@link #type()} reports the corresponding {@link EirfType}; {@link EirfType#ABSENT}
 * marks a document the column has no value for. The cursor is forward-only and not reusable.
 */
public interface SourceColumnCursor {

    /** Advances to the next document. Returns {@code false} once all {@code docCount} documents have been visited. */
    boolean advance();

    /**
     * The {@link EirfType} byte of the current document, or {@link EirfType#ABSENT} when the column has
     * no value for it. Only valid after a successful {@link #advance()}.
     */
    byte type();

    /** The {@code long} value of the current document; valid only when {@link #type()} is {@link EirfType#LONG}. */
    long longValue();

    /** The {@code double} value of the current document; valid only when {@link #type()} is {@link EirfType#DOUBLE}. */
    double doubleValue();

    /** The UTF-8 string value of the current document; valid only when {@link #type()} is {@link EirfType#STRING}. */
    Text stringValue();
}
