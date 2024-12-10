/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.engine.Engine;

/**
 * An interface to allow performing an action when parsing and indexing has been completed
 *
 * TODO: Should this be dropped in favor of {@link org.elasticsearch.index.shard.IndexingOperationListener}?
 */
public interface DocumentSizeReporter {
    /**
     * a default noop implementation
     */
    DocumentSizeReporter EMPTY_INSTANCE = new DocumentSizeReporter() {
    };

    /**
     * An action to be performed upon finished parsing.
     * Note: Corresponds to {@link org.elasticsearch.index.shard.IndexingOperationListener#preIndex}
     */
    default void onParsingCompleted(Engine.Index index) {}

    /**
     * An action to be performed upon finished indexing.
     * Note: Corresponds to {@link org.elasticsearch.index.shard.IndexingOperationListener#postIndex}
     */
    default void onIndexingCompleted(Engine.Index index) {}
}
