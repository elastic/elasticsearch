/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentParser;

/**
 * An interface to allow wrapping an XContentParser and observe the events emitted while parsing
 * A default implementation returns a noop DocumentSizeObserver
 */
public interface DocumentSizeObserver extends NormalizedDocumentSize {
    /**
     * a default noop implementation
     */
    DocumentSizeObserver NOOP = new DocumentSizeObserver() {
        @Override
        public long ingestedBytes() {
            return 0;
        }

        @Override
        public long storedBytes() {
            return 0;
        }
    };

    /**
     * Optionally decorates an xContentParser with additional logic.
     */
    default XContentParser wrapParser(XContentParser xContentParser) {
        return xContentParser;
    }

    /**
     * May enrich the index request with the number of bytes observed when parsing a document.
     */
    default void setNormalisedBytesParsedOn(IndexRequest indexRequest) {}

}
