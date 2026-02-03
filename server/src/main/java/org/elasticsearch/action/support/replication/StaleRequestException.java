/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * An exception indicating a stale request during resharding.
 */
public class StaleRequestException extends ElasticsearchException {

    public StaleRequestException(String index) {
        super("Request for index [{}] is stale due to concurrent reshard operation, retry after sometime", index);
    }

    public StaleRequestException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
