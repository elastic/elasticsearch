/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;

/**
 * A {@code RankDocShardInfo} holds all the final rank documents that exist in a shard. We pass this
 * to fetchPhase so that we can pass all the needed information for the RankBuilder to perform any actions needed
 * when building the final SearchHits (e.g. explain).
 */
public class RankDocShardInfo implements Writeable {

    // doc-id to RankDoc mapping
    private final Map<Integer, RankDoc> rankDocs;

    public RankDocShardInfo(Map<Integer, RankDoc> rankDocs) {
        this.rankDocs = rankDocs;
    }

    public RankDocShardInfo(StreamInput in) throws IOException {
        rankDocs = in.readMap(StreamInput::readVInt, v -> v.readNamedWriteable(RankDoc.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(rankDocs, StreamOutput::writeVInt, StreamOutput::writeNamedWriteable);
    }

    public RankDoc get(int index) {
        return rankDocs.get(index);
    }
}
