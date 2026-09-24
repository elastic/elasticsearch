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
 * Gives the mapping layer one view of a document's {@code _source}, in whichever form its producer
 * already holds it: the x-content bytes that arrived on the request ({@link BytesSource}), or one
 * row of a column-major batch ({@link RowSource}).
 *
 * <p>Answers the same questions for both forms, so the mapping layer and the indexing chain work
 * against this interface whichever form they were handed. Seals the set of implementations, so every
 * source is exactly one of the two.
 *
 * <p>Describes the source of a single document. A batch of documents is a
 * {@link org.elasticsearch.sourcebatch.SourceBatch}, and each {@link RowSource} borrows one of its
 * rows.
 */
// TODO: Eventually will want to combine this with our other source abstractions IndexSource, etc.
public sealed interface DocumentSource permits BytesSource, RowSource {

    /**
     * Names the x-content type in which {@link #originalBytes()} expresses this document.
     */
    XContentType xContentType();

    /**
     * Reports whether this source carries any x-content to parse. A document with no fields, such
     * as {@code {}}, carries content; a source that arrived as zero bytes carries none. Matches the
     * meaning of {@link org.elasticsearch.rest.RestRequest#hasContent()} at the ingest boundary,
     * and differs from {@link org.elasticsearch.sourcebatch.SourceRow#isEmpty()}, which reports a
     * row with no field values.
     */
    boolean hasContent();

    /**
     * Returns a stand-in for this document's serialized size, computed without materializing the
     * x-content bytes. Byte-backed sources report their exact length. Row-backed sources report the
     * size of the row's encoded values, which can sit far from the serialized size, down to zero for
     * a document that carries content, and take time proportional to the number of columns in the
     * batch's schema. Use {@link #hasContent()} to tell whether there is anything to parse.
     */
    int estimatedSizeInBytes();

    /**
     * Opens a parser over this document. Callers own the returned parser and must close it.
     */
    XContentParser parser(XContentParserConfiguration configuration) throws IOException;

    /**
     * Returns the document as x-content bytes in {@link #xContentType()}. Row-backed sources serialize
     * on the first call and cache the result, so prefer {@link #parser} or
     * {@link #estimatedSizeInBytes} where either will do.
     */
    BytesReference originalBytes();
}
