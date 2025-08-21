/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.mapper.ParsedDocument;

/**
 * An interface to allow performing an action when parsing and indexing has been completed
 */
public interface DocumentSizeReporter {
    /**
     * a default noop implementation
     */
    DocumentSizeReporter EMPTY_INSTANCE = new DocumentSizeReporter() {
    };

    /**
     * An action to be performed upon finished indexing.
     */
    default void onParsingCompleted(ParsedDocument parsedDocument) {}

    /**
     * An action to be performed upon finished indexing.
     */
    default void onIndexingCompleted(ParsedDocument parsedDocument) {}
}
