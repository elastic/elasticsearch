/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class SeachableSnapshotsStatsRequest extends BaseNodesRequest<SeachableSnapshotsStatsRequest> {

    SeachableSnapshotsStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SeachableSnapshotsStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    static class NodeStatsRequest extends BaseNodeRequest {

        NodeStatsRequest() {
        }

        NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
