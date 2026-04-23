/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Reindex (or a resume-reindex worker) lost the source scroll or point-in-time context
 * that it was responsible for keeping. This is a server-side failure, so a HTTP status 500
 * (see {@link #status()}) is appropriate.
 */
public class ReindexSourceSearchContextLostException extends ElasticsearchException {

    public static final TransportVersion REINDEX_SOURCE_SEARCH_CONTEXT_LOST_VERSION = TransportVersion.fromName(
        "reindex_source_search_context_lost"
    );

    public ReindexSourceSearchContextLostException(StreamInput in) throws IOException {
        super(in);
    }

    public ReindexSourceSearchContextLostException(Throwable cause) {
        super("Reindex source search context is no longer available", cause);
    }

    @Override
    public RestStatus status() {
        return RestStatus.INTERNAL_SERVER_ERROR;
    }
}
