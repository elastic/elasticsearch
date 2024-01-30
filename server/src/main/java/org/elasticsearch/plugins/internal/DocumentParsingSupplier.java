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
public interface DocumentParsingSupplier {
    DocumentParsingSupplier EMPTY_INSTANCE = new DocumentParsingSupplier() {
        @Override
        public DocumentParsingObserver getNewObserver() {
            return DocumentParsingObserver.EMPTY_INSTANCE;
        }

        @Override
        public DocumentParsingObserver forAlreadyParsedInIngest(long normalisedBytesParsed) {
            return DocumentParsingObserver.EMPTY_INSTANCE;
        }

        @Override
        public DocumentParsingReporter getDocumentParsingReporter() {
            return DocumentParsingReporter.EMPTY_INSTANCE;
        }
    };

    /**
     * @return a new 'empty' observer to use when observing parsing
     */
    DocumentParsingObserver getNewObserver();

    /**
     * @return an observer to use when continue observing parsing based on previous result
     */
    DocumentParsingObserver forAlreadyParsedInIngest(long normalisedBytesParsed);

    /**
     * @return an instance of a reporter to use when parsing has been completed and indexing successful
     */
    DocumentParsingReporter getDocumentParsingReporter();
}
