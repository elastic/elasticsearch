/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

//needed for injection
public interface DocumentParsingObserverSupplier {
    DocumentParsingObserverSupplier EMPTY_INSTANCE = new DocumentParsingObserverSupplier() {
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

    DocumentParsingObserver getNewObserver();

    DocumentParsingObserver forAlreadyParsedInIngest(long normalisedBytesParsed);

    DocumentParsingReporter getDocumentParsingReporter();
}
