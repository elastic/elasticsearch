/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

final class ClearRepositoriesStatsArchiveNodeResponse extends BaseNodeResponse {
    ClearRepositoriesStatsArchiveNodeResponse(StreamInput in) throws IOException {
        super(in);
    }

    ClearRepositoriesStatsArchiveNodeResponse(DiscoveryNode node) {
        super(node);
    }
}
