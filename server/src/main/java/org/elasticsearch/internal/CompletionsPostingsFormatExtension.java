/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.internal;

import org.apache.lucene.search.suggest.document.CompletionPostingsFormat;

/**
 * Allows plugging-in the Completions Postings Format.
 */
public interface CompletionsPostingsFormatExtension {

    /**
     * Returns the name of the  {@link CompletionPostingsFormat} that Elasticsearch should use. Should return null if the extension
     * is not enabled.
     * <p>
     * Note that the name must match a codec that is available on all nodes in the cluster, otherwise IndexCorruptionExceptions will occur.
     * A feature can be used to protect against this scenario, or alternatively, the codec code can be rolled out prior to its usage by this
     * extension.
     */
    String getFormatName();
}
