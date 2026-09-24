/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds a document as the x-content bytes its producer received.
 *
 * @param originalBytes       the document, normalized to an array-backed {@link BytesReference}
 * @param xContentType        the x-content type of {@code originalBytes}
 * @param includeSourceOnError whether parse failures may quote the offending source
 */
public record BytesSource(BytesReference originalBytes, XContentType xContentType, boolean includeSourceOnError) implements DocumentSource {

    /**
     * Stands in for a document whose source travels separately from it, carrying no content and
     * reporting a size of zero.
     */
    public static final BytesSource EMPTY = new BytesSource(BytesArray.EMPTY, XContentType.JSON, false);

    public BytesSource {
        Objects.requireNonNull(originalBytes);
        Objects.requireNonNull(xContentType);
        // we always convert back to byte array, since we store it and Field only supports bytes.
        // so, we might as well do it here, and improve the performance of working with direct byte arrays.
        originalBytes = originalBytes.hasArray() ? originalBytes : new BytesArray(originalBytes.toBytesRef());
    }

    @Override
    public boolean hasContent() {
        return originalBytes.length() > 0;
    }

    @Override
    public int estimatedSizeInBytes() {
        return originalBytes.length();
    }

    @Override
    public XContentParser parser(XContentParserConfiguration configuration) throws IOException {
        return XContentHelper.createParser(configuration.withIncludeSourceOnError(includeSourceOnError), originalBytes, xContentType);
    }
}
