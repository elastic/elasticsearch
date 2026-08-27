/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;

import java.io.Closeable;

/**
 * Abstraction over the Tika extraction mechanism. Two implementations are provided:
 * <ul>
 *   <li>{@link LocalExtractionBackend} – runs Apache Tika in the current JVM (default, unchanged
 *   behaviour).</li>
 *   <li>{@link TikaServerExtractionBackend} – delegates extraction to an external tika-server over
 *   HTTP.</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe and are shared across all {@link AttachmentProcessor}
 * instances on a node.
 */
interface ExtractionBackend extends Closeable {

    /**
     * Extract content and metadata from {@code content}.
     *
     * <p>The listener is invoked exactly once: with the result on success, or with an exception on
     * failure. {@link LocalExtractionBackend} always calls the listener <em>synchronously</em>
     * (i.e. before this method returns).
     *
     * @param content      raw document bytes
     * @param resourceName optional filename hint used for content-type detection (may be {@code null})
     * @param maxChars     maximum number of extracted characters; {@code -1} means unlimited
     * @param listener     receives the {@link ExtractionResult} or an exception
     */
    void extract(byte[] content, @Nullable String resourceName, int maxChars, ActionListener<ExtractionResult> listener);
}
