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
        public DocumentSizeObserver newFixedDocumentSizeObserver() {
            return DocumentSizeObserver.EMPTY_INSTANCE;
        }

        @Override
        public DocumentSizeReporter getDocumentParsingReporter() {
            return DocumentSizeReporter.EMPTY_INSTANCE;
        }

        @Override
        public DocumentSizeObserver newFixedDocumentSizeObserver(long normalisedBytesParsed) {
            return DocumentSizeObserver.EMPTY_INSTANCE;
        }
    };

    /**
     * @return a new 'empty' observer to use when observing parsing
     */
    DocumentSizeObserver newFixedDocumentSizeObserver();

    /**
     * @return an observer with a fixed observed value.
     */
    DocumentSizeObserver newFixedDocumentSizeObserver(long normalisedBytesParsed);

    /**
     * @return an instance of a reporter to use when parsing has been completed and indexing successful
     */
    DocumentSizeReporter getDocumentParsingReporter();

}
