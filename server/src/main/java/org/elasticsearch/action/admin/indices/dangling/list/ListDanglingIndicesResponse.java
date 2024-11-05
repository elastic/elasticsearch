/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.dangling.list;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Models a response to a {@link ListDanglingIndicesRequest}. A list request queries every node in the
 * cluster and aggregates their responses. When the aggregated response is converted to {@link XContent},
 * information for each dangling index is presented under the "dangling_indices" key. If any nodes
 * in the cluster failed to answer, the details are presented under the "_nodes.failures" key.
 */
public class ListDanglingIndicesResponse extends BaseNodesResponse<NodeListDanglingIndicesResponse> implements ToXContentObject {

    public ListDanglingIndicesResponse(
        ClusterName clusterName,
        List<NodeListDanglingIndicesResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    public RestStatus status() {
        return this.hasFailures() ? RestStatus.INTERNAL_SERVER_ERROR : RestStatus.OK;
    }

    // Visible for testing
    static Collection<AggregatedDanglingIndexInfo> resultsByIndexUUID(List<NodeListDanglingIndicesResponse> nodes) {
        Map<String, AggregatedDanglingIndexInfo> byIndexUUID = new HashMap<>();

        for (NodeListDanglingIndicesResponse nodeResponse : nodes) {
            for (DanglingIndexInfo info : nodeResponse.getDanglingIndices()) {
                final String indexUUID = info.getIndexUUID();

                final AggregatedDanglingIndexInfo aggregatedInfo = byIndexUUID.computeIfAbsent(
                    indexUUID,
                    (_uuid) -> new AggregatedDanglingIndexInfo(indexUUID, info.getIndexName(), info.getCreationDateMillis())
                );

                aggregatedInfo.nodeIds.add(info.getNodeId());
            }
        }

        return byIndexUUID.values();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("dangling_indices");

        for (AggregatedDanglingIndexInfo info : resultsByIndexUUID(this.getNodes())) {
            builder.startObject();

            builder.field("index_name", info.indexName);
            builder.field("index_uuid", info.indexUUID);
            builder.timestampFieldsFromUnixEpochMillis("creation_date_millis", "creation_date", info.creationDateMillis);

            builder.array("node_ids", info.nodeIds.toArray(new String[0]));

            builder.endObject();
        }

        return builder.endArray();
    }

    @Override
    protected List<NodeListDanglingIndicesResponse> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeListDanglingIndicesResponse> nodes) throws IOException {
        TransportAction.localOnly();
    }

    // visible for testing
    static class AggregatedDanglingIndexInfo {
        private final String indexUUID;
        private final String indexName;
        private final long creationDateMillis;
        private final List<String> nodeIds;

        AggregatedDanglingIndexInfo(String indexUUID, String indexName, long creationDateMillis) {
            this.indexUUID = indexUUID;
            this.indexName = indexName;
            this.creationDateMillis = creationDateMillis;
            this.nodeIds = new ArrayList<>();
        }

        // the methods below are used in the unit tests

        public List<String> getNodeIds() {
            return nodeIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AggregatedDanglingIndexInfo that = (AggregatedDanglingIndexInfo) o;
            return creationDateMillis == that.creationDateMillis
                && indexUUID.equals(that.indexUUID)
                && indexName.equals(that.indexName)
                && nodeIds.equals(that.nodeIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexUUID, indexName, creationDateMillis, nodeIds);
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "AggregatedDanglingIndexInfo{indexUUID='%s', indexName='%s', creationDateMillis=%d, nodeIds=%s}",
                indexUUID,
                indexName,
                creationDateMillis,
                nodeIds
            );
        }
    }
}
