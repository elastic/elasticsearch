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
        public void parsingFinished(String indexName) {
        }
    };

    /**
     * Decorates a provided xContentParser with additional logic (gather some state).
     * The Decorator parser should use a state from DocumentParsingObserver
     * in order to allow a parsingFinished method to use that state
     *
     * @param xContentParser to be decorated
     * @return a decorator xContentParser
     */
    XContentParser wrapParser(XContentParser xContentParser);

    /**
     * Reports (perhaps to some external components) the state that was gathered by a decorated wrap
     *
     * @param indexName an index name that is associated with the parsed document
     */
    void parsingFinished(String indexName);
}
