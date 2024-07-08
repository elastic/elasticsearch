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
public interface DocumentSizeObserver extends NormalisedBytesWrapper {
    /**
     * a default noop implementation
     */
    DocumentSizeObserver EMPTY_INSTANCE = new DocumentSizeObserver() {
        @Override
        public long raiNormalisedBytes() {
            return 0;
        }

        @Override
        public long rasNormalisedBytes() {
            return 0;
        }

        @Override
        public XContentParser wrapParser(XContentParser xContentParser) {
            return xContentParser;
        }

        @Override
        public void setNormalisedBytesParsedOn(IndexRequest indexRequest) {}
    };

    /**
     * Decorates a provided xContentParser with additional logic (gather some state).
     *
     * @param xContentParser to be decorated
     * @return a decorator xContentParser
     */
    XContentParser wrapParser(XContentParser xContentParser);

    /**
     * Enriches the index request with the number of bytes observed when parsing a document
     * @param indexRequest
     */
    void setNormalisedBytesParsedOn(IndexRequest indexRequest);

}
