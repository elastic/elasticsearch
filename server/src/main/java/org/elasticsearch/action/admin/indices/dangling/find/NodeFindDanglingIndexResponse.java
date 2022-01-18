/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.find;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Used when querying every node in the cluster for a specific dangling index.
 */
public class NodeFindDanglingIndexResponse extends BaseNodeResponse {
    /**
     * A node could report several dangling indices. This class will contain them all.
     * A single node could even multiple different index versions for the same index
     * UUID if the situation is really crazy, though perhaps this is more likely
     * when collating responses from different nodes.
     */
    private final List<IndexMetadata> danglingIndexInfo;

    public List<IndexMetadata> getDanglingIndexInfo() {
        return this.danglingIndexInfo;
    }

    public NodeFindDanglingIndexResponse(DiscoveryNode node, List<IndexMetadata> danglingIndexInfo) {
        super(node);
        this.danglingIndexInfo = danglingIndexInfo;
    }

    protected NodeFindDanglingIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.danglingIndexInfo = in.readList(IndexMetadata::readFrom);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(this.danglingIndexInfo);
    }
}
