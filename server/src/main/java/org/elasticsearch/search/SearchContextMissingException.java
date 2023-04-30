/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;

public class SearchContextMissingException extends ElasticsearchException {

    private final ShardSearchContextId contextId;

    public SearchContextMissingException(ShardSearchContextId contextId) {
        super("No search context found for id [" + contextId.getId() + "]");
        this.contextId = contextId;
    }

    public ShardSearchContextId contextId() {
        return this.contextId;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    public SearchContextMissingException(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        contextId.writeTo(out);
    }
}
