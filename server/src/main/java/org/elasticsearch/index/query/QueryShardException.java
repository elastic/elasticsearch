/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception that is thrown when creating lucene queries on the shard
 */
public final class QueryShardException extends ElasticsearchException {

    public QueryShardException(QueryRewriteContext context, String msg, Object... args) {
        this(context, msg, null, args);
    }

    public QueryShardException(QueryRewriteContext context, String msg, Throwable cause, Object... args) {
        this(context.getFullyQualifiedIndex(), msg, cause, args);
    }

    /**
     * This constructor is provided for use in unit tests where a
     * {@link SearchExecutionContext} may not be available
     */
    public QueryShardException(Index index, String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
        setIndex(index);
    }

    public QueryShardException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
