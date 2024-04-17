/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.mapper.LuceneDocument;

/**
 * An interface to allow performing an action when parsing has been completed and successful
 */
public interface DocumentSizeReporter {
    /**
     * a default noop implementation
     */
    DocumentSizeReporter EMPTY_INSTANCE = new DocumentSizeReporter() {
        @Override
        public void onIndexingCompleted(String indexName, long normalizedBytesParsed) {

        }

        @Override
        public void onParsingCompleted(LuceneDocument luceneDocument, long normalizedBytesParsed) {

        }
    };

    /**
     * An action to be performed upon finished parsing.
     */
    void onIndexingCompleted(String indexName, long normalizedBytesParsed);

    void onParsingCompleted(LuceneDocument luceneDocument, long normalizedBytesParsed);

}
