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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class SeekStatsResponse extends BaseNodesResponse<SeekStats> implements ToXContentObject {

    public SeekStatsResponse(ClusterName clusterName, List<SeekStats> seekStats, List<FailedNodeException> failures) {
        super(clusterName, seekStats, failures);
    }

    public SeekStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<SeekStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(SeekStats::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<SeekStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        for (SeekStats seekStats : getNodes()) {
            builder.startObject(seekStats.getNode().getId());
            seekStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
