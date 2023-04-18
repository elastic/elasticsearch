/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SeekStatsResponse extends BaseNodesResponse<NodeSeekStats> implements ToXContentObject {

    public SeekStatsResponse(ClusterName clusterName, List<NodeSeekStats> seekStats, List<FailedNodeException> failures) {
        super(clusterName, seekStats, failures);
    }

    public SeekStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<NodeSeekStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeSeekStats::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeSeekStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        for (NodeSeekStats seekStats : getNodes()) {
            builder.startObject(seekStats.getNode().getId());
            seekStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public Map<String, List<ShardSeekStats>> getSeekStats() {
        Map<String, List<ShardSeekStats>> combined = new HashMap<>();
        for (NodeSeekStats nodeSeekStats : getNodes()) {
            nodeSeekStats.getSeekStats()
                .forEach((index, shardSeekStats) -> combined.computeIfAbsent(index, k -> new ArrayList<>()).addAll(shardSeekStats));
        }
        return combined;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
