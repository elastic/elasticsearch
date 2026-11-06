/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.truncate;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A request to discard all documents of one or more indices while keeping the indices themselves: their settings, mappings and
 * aliases are untouched. Document history is discarded as well, so sequence numbers keep increasing but versions of documents
 * indexed afterwards start over.
 */
public class TruncateIndexRequest extends BroadcastRequest<TruncateIndexRequest> {

    public TruncateIndexRequest(String... indices) {
        super(indices);
    }

    public TruncateIndexRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String toString() {
        return "TruncateIndexRequest{indices=" + String.join(",", indices()) + "}";
    }
}
