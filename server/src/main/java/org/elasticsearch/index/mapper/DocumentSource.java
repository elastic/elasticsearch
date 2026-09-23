/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

/**
 * A document's {@code _source} in whichever representation its producer already holds: the raw
 * x-content bytes that arrived on the request ({@link BytesSource}), or one row of a column-major
 * batch ({@link RowSource}).
 *
 * <p>Both representations answer the same questions, so the mapping layer and the indexing chain
 * work against this interface and stay agnostic of which one they were handed. Keeping the set
 * closed makes "a source has exactly one representation" a property of the type, rather than an
 * invariant each reader re-establishes by testing fields for absence.
 *
 * <p>Implementations describe the source of a single document. A batch of documents is a
 * {@link org.elasticsearch.sourcebatch.SourceBatch}, from which each {@link RowSource} borrows one
 * row.
 */
// TODO: Eventually will want to combine this with our other source abstractions IndexSource, etc.
public sealed interface DocumentSource permits BytesSource, RowSource {

    XContentType xContentType();

    boolean isEmpty();

    /**
     * A cheap stand-in for this document's serialized size, for accounting that must not pay to
     * materialize the x-content bytes. Byte-backed sources report their exact length; row-backed
     * sources report the row's variable-length payload, which counts neither fixed-width values nor
     * values held entirely in column metadata, and so reports zero for a row of only booleans.
     */
    int estimatedSizeInBytes();

    /**
     * Opens a parser over this document. Callers own the returned parser and must close it.
     */
    XContentParser parser(XContentParserConfiguration configuration) throws IOException;

    /**
     * The document as x-content bytes in {@link #xContentType()}. Row-backed sources serialize on
     * the first call and cache the result, so prefer {@link #parser} or
     * {@link #estimatedSizeInBytes} where either will do.
     */
    BytesReference originalBytes();
}
