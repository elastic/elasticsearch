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
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * An exception indicating a stale request during resharding.
 */
public class StaleRequestException extends ElasticsearchException {

    private static final String STALE_SUMMARY = "es.stale_summary";

    /**
     * Construct a StaleRequestException
     * @param shardId      The shardId of the shard that triggered this exception.
     * @param staleSummary The stale summary that triggered this exception.
     *                     The caller should retry when the request summary it produces differs.
     */
    public StaleRequestException(ShardId shardId, SplitShardCountSummary staleSummary) {
        super("Request is stale due to concurrent reshard operation, retry later", shardId);
        setStaleSummary(staleSummary);
        setShard(shardId);
    }

    public StaleRequestException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }

    private void setStaleSummary(SplitShardCountSummary staleSummary) {
        addMetadata(STALE_SUMMARY, Integer.toString(staleSummary.asInt()));
    }

    public SplitShardCountSummary getStaleSummary() {
        final var shardCountMetadata = getMetadata(STALE_SUMMARY);
        // may be null if the exception was produced by a node running an older version
        if (shardCountMetadata == null) {
            return SplitShardCountSummary.UNSET;
        } else {
            assert shardCountMetadata.size() == 1;
            return SplitShardCountSummary.fromInt(Integer.parseInt(shardCountMetadata.getFirst()));
        }
    }
}
