/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

/**
 * Exposes the normalized ingested and stored size of a document.
 */
public interface NormalizedDocumentSize {

    /**
     * The ingest size of the document.
     */
    long ingestedBytes();

    /**
     * The stored retained size of the document.
     */
    long storedBytes();
}
