/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.list;

import org.elasticsearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Used when querying every node in the cluster for dangling indices, in response to a list request.
 */
public class NodeListDanglingIndicesResponse extends BaseNodeResponse {
    private final List<DanglingIndexInfo> indexMetaData;

    public List<DanglingIndexInfo> getDanglingIndices() {
        return this.indexMetaData;
    }

    public NodeListDanglingIndicesResponse(DiscoveryNode node, List<DanglingIndexInfo> indexMetaData) {
        super(node);
        this.indexMetaData = indexMetaData;
    }

    protected NodeListDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);
        this.indexMetaData = in.readList(DanglingIndexInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(this.indexMetaData);
    }
}
