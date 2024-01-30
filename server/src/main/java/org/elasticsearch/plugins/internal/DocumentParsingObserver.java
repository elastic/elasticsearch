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
 * A default implementation returns a noop DocumentParsingObserver - does not wrap a XContentParser and
 * does not do anything upon finishing parsing.
 */
public interface DocumentParsingObserver {
    /**
     * a default noop implementation
     */
    DocumentParsingObserver EMPTY_INSTANCE = new DocumentParsingObserver() {
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
     * The Decorator parser should use a state from DocumentParsingObserver
     * in order to perform an action upon finished parsing which will be aware of the state
     * gathered during parsing
     *
     * @param xContentParser to be decorated
     * @return a decorator xContentParser
     */
    XContentParser wrapParser(XContentParser xContentParser);

    long normalisedBytesParsed();
}
