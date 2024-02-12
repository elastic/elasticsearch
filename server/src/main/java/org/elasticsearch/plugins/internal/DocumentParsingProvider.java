/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

/**
 * An interface to provide instances of document parsing observer and reporter
 */
public interface DocumentParsingProvider {
    DocumentParsingProvider EMPTY_INSTANCE = new DocumentParsingProvider() {
        @Override
        public DocumentSizeObserver newDocumentSizeObserver() {
            return DocumentSizeObserver.EMPTY_INSTANCE;
        }

        @Override
        public DocumentSizeReporter getDocumentParsingReporter() {
            return DocumentSizeReporter.EMPTY_INSTANCE;
        }

        @Override
        public DocumentSizeObserver newFixedSizeDocumentObserver(long normalisedBytesParsed) {
            return DocumentSizeObserver.EMPTY_INSTANCE;
        }
    };

    /**
     * @return a new 'empty' observer to use when observing parsing
     */
    DocumentSizeObserver newDocumentSizeObserver();

    /**
     * @return an observer with a previously observed value (fixed to this value, not continuing)
     */
    DocumentSizeObserver newFixedSizeDocumentObserver(long normalisedBytesParsed);

    /**
     * @return an instance of a reporter to use when parsing has been completed and indexing successful
     */
    DocumentSizeReporter getDocumentParsingReporter();

}
