/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.metering.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetBlobStoreStatsNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private final RepositoryStats repositoryStats;

    public GetBlobStoreStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.repositoryStats = new RepositoryStats(in);
    }

    public GetBlobStoreStatsNodeResponse(DiscoveryNode node, RepositoryStats repositoryStats) {
        super(node);
        this.repositoryStats = repositoryStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(getNode().getId());
        builder.field("object_store_stats", repositoryStats.requestCounts);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        repositoryStats.writeTo(out);
    }

    public RepositoryStats getRepositoryStats() {
        return repositoryStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetBlobStoreStatsNodeResponse that = (GetBlobStoreStatsNodeResponse) o;
        return Objects.equals(getNode().getId(), that.getNode().getId()) && Objects.equals(repositoryStats, that.repositoryStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNode().getId(), repositoryStats);
    }

    @Override
    public String toString() {
        return "GetBlobStoreStatsNodeResponse{" + "nodeId=" + getNode().getId() + ", repositoryStats=" + repositoryStats + '}';
    }
}
