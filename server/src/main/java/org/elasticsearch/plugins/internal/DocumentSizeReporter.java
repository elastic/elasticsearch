/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.mapper.LuceneDocument;

import java.util.Map;

/**
 * An interface to allow performing an action when parsing has been completed and successful
 */
public interface DocumentSizeReporter {
    /**
     * a default noop implementation
     */
    DocumentSizeReporter EMPTY_INSTANCE = new DocumentSizeReporter() {
        @Override
        public void onIndexingCompleted(long normalizedBytesParsed) {

        }

        @Override
        public void onParsingCompleted(LuceneDocument luceneDocument, long normalizedBytesParsed) {

        }
    };

    /**
     * An action to be performed upon finished parsing.
     */
    void onIndexingCompleted(long normalizedBytesParsed);

    void onParsingCompleted(LuceneDocument luceneDocument, long normalizedBytesParsed);

    default Map<String, String> getAsUserData(Map<String, String> prevUserDataMap) {
        return Map.of();
    }

}
