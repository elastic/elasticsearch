/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

/**
 * An interface to allow wrapping an XContentParser and observe the events emitted while parsing
 * A default implementation returns a noop DocumentParsingObserver - does not wrap a XContentParser and
 * does not do anything upon finishing parsing.
 */
public interface DocumentParsingReporter {
    /**
     * a default noop implementation
     */
    DocumentParsingReporter EMPTY_INSTANCE = new DocumentParsingReporter() {

        @Override
        public void onCompleted(String indexName, long normalizedBytesParsed) {}

    };


    /**
     * An action to be performed upon finished parsing.
     */
    void onCompleted(String indexName, long normalizedBytesParsed);

}
