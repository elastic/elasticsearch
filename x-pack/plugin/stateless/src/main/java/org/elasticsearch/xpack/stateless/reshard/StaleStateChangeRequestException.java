/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.ElasticsearchException;

/// Signals that the request to change resharding-related state of the shard is stale.
public class StaleStateChangeRequestException extends ElasticsearchException {
    public StaleStateChangeRequestException(String msg, Object... args) {
        super(msg, args);
    }
}
