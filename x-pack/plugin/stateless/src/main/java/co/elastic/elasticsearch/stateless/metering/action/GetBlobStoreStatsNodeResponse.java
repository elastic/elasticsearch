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

import org.elasticsearch.TransportVersions;
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
    private final RepositoryStats obsRepositoryStats;

    public GetBlobStoreStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.repositoryStats = new RepositoryStats(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_500_056)) {
            this.obsRepositoryStats = new RepositoryStats(in);
        } else {
            this.obsRepositoryStats = RepositoryStats.EMPTY_STATS;
        }
    }

    public GetBlobStoreStatsNodeResponse(DiscoveryNode node, RepositoryStats repositoryStats, RepositoryStats obsRepositoryStats) {
        super(node);
        this.repositoryStats = repositoryStats;
        this.obsRepositoryStats = obsRepositoryStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(getNode().getId());
        {
            builder.startObject("object_store_stats");
            builder.field("request_counts", repositoryStats.requestCounts);
            builder.endObject();
        }
        {
            builder.startObject("operational_backup_service_stats");
            builder.field("request_counts", obsRepositoryStats.requestCounts);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        repositoryStats.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_500_056)) {
            obsRepositoryStats.writeTo(out);
        }
    }

    public RepositoryStats getRepositoryStats() {
        return repositoryStats;
    }

    public RepositoryStats getObsRepositoryStats() {
        return obsRepositoryStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetBlobStoreStatsNodeResponse that = (GetBlobStoreStatsNodeResponse) o;
        return Objects.equals(getNode().getId(), that.getNode().getId())
            && Objects.equals(repositoryStats, that.repositoryStats)
            && Objects.equals(obsRepositoryStats, that.obsRepositoryStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repositoryStats, obsRepositoryStats);
    }

    @Override
    public String toString() {
        return "GetBlobStoreStatsNodeResponse{" + "repositoryStats=" + repositoryStats + ", obsRepositoryStats=" + obsRepositoryStats + '}';
    }
}
