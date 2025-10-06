/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SearchSplitShardCountSummary extends SplitShardCountSummary {
    private static final TransportVersion SEARCH_RESHARD_SHARDCOUNT_SUMMARY = TransportVersion.fromName(
        "search_reshard_shardcount_summary"
    );

    public SearchSplitShardCountSummary(StreamInput in) throws IOException {
        super(readShardCountSummary(in));
    }

    /**
     * Given {@code IndexMetadata} and a {@code shardId}, this method returns the "effective" shard count
     * as seen by this IndexMetadata, for indexing operations.
     *
     * See {@code getReshardSplitShardCountSummary} for more details.
     * @param indexMetadata IndexMetadata of the shard for which we want to calculate the effective shard count
     * @param shardId       Input shardId for which we want to calculate the effective shard count
     */
    public static SearchSplitShardCountSummary fromMetadata(IndexMetadata indexMetadata, int shardId) {
        return new SearchSplitShardCountSummary(
            getReshardSplitShardCountSummary(indexMetadata, shardId, IndexReshardingState.Split.TargetShardState.SPLIT)
        );
    }

    private static int readShardCountSummary(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(SEARCH_RESHARD_SHARDCOUNT_SUMMARY)) {
            return in.readVInt();
        } else {
            return UNSET_VALUE;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(SEARCH_RESHARD_SHARDCOUNT_SUMMARY)) {
            out.writeVInt(shardCountSummary);
        }
    }

    SearchSplitShardCountSummary(int shardCountSummary) {
        super(shardCountSummary);
    }
}
