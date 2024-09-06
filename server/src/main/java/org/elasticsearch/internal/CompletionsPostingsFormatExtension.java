/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.internal;

import org.apache.lucene.search.suggest.document.CompletionPostingsFormat;

/**
 * Allows plugging-in the Postings Format.
 */
public interface CompletionsPostingsFormatExtension {

    /**
     * Returns the name of the  {@link CompletionPostingsFormat} that Elasticsearch should use.
     */
    String getFormatName();

    /**
     * Sets whether this extension is enabled. If the extension is not enabled, {@link #getFormatName()} should return null.
     */
    void setExtensionEnabled(boolean isExtensionEnabled);
}
