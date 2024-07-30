/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.xcontent.XContentParser;

/**
 * An interface to allow wrapping an XContentParser and observe the events emitted while parsing
 * A default implementation returns a noop DocumentSizeObserver
 */
public interface DocumentSizeObserver {
    /**
     * a default noop implementation
     */
    DocumentSizeObserver EMPTY_INSTANCE = new DocumentSizeObserver() {
        @Override
        public XContentParser wrapParser(XContentParser xContentParser) {
            return xContentParser;
        }

        @Override
        public long normalisedBytesParsed() {
            return 0;
        }

    };

    /**
     * Decorates a provided xContentParser with additional logic (gather some state).
     *
     * @param xContentParser to be decorated
     * @return a decorator xContentParser
     */
    XContentParser wrapParser(XContentParser xContentParser);

    /**
     * Returns the state gathered during parsing
     *
     * @return a number representing a state parsed
     */
    long normalisedBytesParsed();

    /**
     * Indicates if an observer was used on an update request with script
     *
     * @return true if update was done by script, false otherwise
     */
    default boolean isUpdateByScript() {
        return false;
    }
}
