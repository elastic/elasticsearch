/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.exception.ElasticsearchException;

public class DownsampleShardIndexerException extends ElasticsearchException {
    private final boolean retriable;

    public DownsampleShardIndexerException(final Throwable cause, final String message, boolean retriable) {
        super(message, cause);
        this.retriable = retriable;
    }

    public DownsampleShardIndexerException(final String message, boolean retriable) {
        super(message);
        this.retriable = retriable;
    }

    public boolean isRetriable() {
        return retriable;
    }
}
